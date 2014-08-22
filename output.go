package nsq

import (
	"errors"
	"fmt"
	"github.com/bitly/go-hostpool"
	"github.com/bitly/go-nsq"
	"github.com/mozilla-services/heka/pipeline"
	"sync/atomic"
	"time"
)

const (
	ModeRoundRobin = iota
	ModeHostPool
)

type NsqOutputConfig struct {
	Addresses      []string `toml:"addresses"`
	Topic          string   `toml:"topic"`
	Mode           string   `toml:"mode"`
	RetryDelay     uint64   `toml:"retry_delay"`
	RetryQueueSize uint64   `toml:"retry_queue_size"`
	MaxReconnects  uint64   `toml:"max_reconnects"`
	MaxMsgRetries  uint64   `toml:"max_msg_retries"`
}

type NsqOutput struct {
	*NsqOutputConfig
	Mode       int
	runner     pipeline.OutputRunner
	producers  map[string]*nsq.Producer
	config     *nsq.Config
	respChan   chan *nsq.ProducerTransaction
	counter    uint64
	hostPool   hostpool.HostPool
	retryChan  chan RetryMsg
	retryCount uint64
}

type RetryMsg struct {
	count     uint64
	maxCount  uint64
	Body      []byte
	retryChan chan RetryMsg
}

func (m RetryMsg) Retry() error {
	if m.count > m.maxCount {
		return errors.New("Exceeded max retry attempts sending message. No longer requeuing message.")
	}
	m.retryChan <- m
	m.count++
	return nil
}

func (output *NsqOutput) ConfigStruct() interface{} {
	return &NsqOutputConfig{
		RetryDelay:     3,
		RetryQueueSize: 50,
		MaxReconnects:  3,
	}
}

func (output *NsqOutput) Init(config interface{}) (err error) {
	conf := config.(*NsqOutputConfig)
	output.NsqOutputConfig = conf
	output.config = nsq.NewConfig()

	switch conf.Mode {
	case "round-robin":
		output.Mode = ModeRoundRobin
	case "hostpool":
		output.Mode = ModeHostPool
	}

	output.producers = make(map[string]*nsq.Producer)
	var producer *nsq.Producer
	for _, addr := range output.Addresses {
		producer, err = nsq.NewProducer(addr, output.config)
		if err != nil {
			break
		}
		producer.SetLogger(nil, nsq.LogLevelError)
		output.producers[addr] = producer
	}

	// Cleanup all successful producer creations on error
	if err != nil {
		fmt.Println("error")
		for _, producer = range output.producers {
			if producer != nil {
				producer.Stop()
			}
		}
		return err
	}

	output.respChan = make(chan *nsq.ProducerTransaction, len(output.Addresses))
	output.retryChan = make(chan RetryMsg, output.RetryQueueSize)
	output.hostPool = hostpool.New(output.Addresses)
	for i := 0; i < len(output.Addresses); i++ {
		go output.responder()
	}

	return nil
}

func (output *NsqOutput) Run(runner pipeline.OutputRunner,
	helper pipeline.PluginHelper) (err error) {
	if runner.Encoder() == nil {
		return errors.New("Encoder required.")
	}

	var (
		pack     *pipeline.PipelinePack
		outgoing []byte
		delay    <-chan time.Time
		msg      RetryMsg
	)

	output.runner = runner
	inChan := runner.InChan()
	ok := true

	defer output.cleanup()

	for ok {
		if delay != nil {
			<-delay
			delay = nil
		}
		select {
		case pack, ok = <-inChan:
			if !ok {
				return nil
			}
			outgoing, err = output.runner.Encode(pack)
			if err != nil {
				runner.LogError(err)
			} else {
				err = output.sendMessage(outgoing)
				if err != nil {
					delay, err = output.handleSendErr(err)
					if err != nil {
						return
					}
					// Create a retry msg, and requeue it
					msg := RetryMsg{Body: outgoing, retryChan: output.retryChan, maxCount: output.MaxMsgRetries}
					err = msg.Retry()
					if err != nil {
						output.runner.LogError(err)
					}
				}
			}
			pack.Recycle()
		case msg, ok = <-output.retryChan:
			if !ok {
				return nil
			}
			err = output.sendMessage(msg.Body)
			if err != nil {
				delay, err = output.handleSendErr(err)
				if err != nil {
					return
				}
				// requeue the message
				err = msg.Retry()
				if err != nil {
					output.runner.LogError(err)
				}
			}
		}
	}

	return nil
}

func (output *NsqOutput) cleanup() {
	close(output.respChan)
	close(output.retryChan)
	for _, producer := range output.producers {
		producer.Stop()
	}
}

// sendMessage asyncronously publishes encoded pipeline packs to nsq. It will
// deliver messages to a corresponding nsq producer. If there is more than one
// producer it uses RoundRobin or HostPool strategies according to the config
// option.
func (output *NsqOutput) sendMessage(body []byte) (err error) {
	switch output.Mode {
	case ModeRoundRobin:
		counter := atomic.AddUint64(&output.counter, 1)
		idx := counter % uint64(len(output.Addresses))
		addr := output.Addresses[idx]
		p := output.producers[addr]
		err = p.PublishAsync(output.Topic, body, output.respChan, body)
	case ModeHostPool:
		hostPoolResponse := output.hostPool.Get()
		p := output.producers[hostPoolResponse.Host()]
		err = p.PublishAsync(output.Topic, body, output.respChan, body, hostPoolResponse)
	}
	if err != nil {
		output.retryCount++
	} else {
		output.retryCount = 0
	}
	return
}

func (output *NsqOutput) handleSendErr(err error) (<-chan time.Time, error) {
	output.runner.LogError(err)
	if output.retryCount >= output.MaxReconnects {
		return nil, errors.New("Exceeded max reconnect attempts")
	}
	// how long we wait before trying to publish again
	return time.After(time.Second * time.Duration(output.RetryDelay)), nil
}

// responder handles the eventual response from the asyncronous publish. If
// there was an error when publishing, then we put the pipeline pack back onto
// outputs inChan so that it can be republished. If there was no error, then the
// pack gets recycled.
func (output *NsqOutput) responder() {
	var (
		body             []byte
		hostPoolResponse hostpool.HostPoolResponse
	)

	for t := range output.respChan {
		switch output.Mode {
		case ModeRoundRobin:
			body = t.Args[0].([]byte)
		case ModeHostPool:
			body = t.Args[0].([]byte)
			hostPoolResponse = t.Args[1].(hostpool.HostPoolResponse)
		}

		success := t.Error == nil

		if hostPoolResponse != nil {
			if success {
				hostPoolResponse.Mark(nil)
			} else {
				hostPoolResponse.Mark(errors.New("failed"))
			}
		}

		if !success {
			output.runner.LogError(fmt.Errorf("Publishing message failed: %s", t.Error.Error()))
			msg := RetryMsg{Body: body, retryChan: output.retryChan, maxCount: output.MaxMsgRetries}
			err := msg.Retry()
			if err != nil {
				output.runner.LogError(fmt.Errorf("%s. No longer attempting to send message.", err.Error()))
			}
		}
	}
}

func init() {
	pipeline.RegisterPlugin("NsqOutput", func() interface{} {
		return new(NsqOutput)
	})
}
