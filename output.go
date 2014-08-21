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
	Addresses []string `toml:"addresses"`
	Topic     string   `toml:"topic"`
	Mode      string   `toml:"mode"`
}

type NsqOutput struct {
	*NsqOutputConfig
	Mode      int
	runner    pipeline.OutputRunner
	producers map[string]*nsq.Producer
	config    *nsq.Config
	respChan  chan *nsq.ProducerTransaction
	counter   uint64
	hostPool  hostpool.HostPool
	retryChan chan RetryMsg
}

type RetryMsg struct {
	count     uint64
	maxCount  uint64
	body      []byte
	retryChan chan RetryMsg
}

func (m RetryMsg) Retry() error {
	if m.count > m.maxCount {
		return errors.New("Exceeded max retry attempts")
	}
	m.retryChan <- m
	m.count++
	return nil
}

func (output *NsqOutput) ConfigStruct() interface{} {
	return &NsqOutputConfig{}
}

func (output *NsqOutput) Init(config interface{}) (err error) {
	conf := config.(*NsqOutputConfig)
	output.NsqOutputConfig = conf
	output.config = nsq.NewConfig()
	// output.config.BackoffMultiplier = time.Duration(time.Second * 6)

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
	output.retryChan = make(chan RetryMsg, 50)
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
	)

	output.runner = runner
	defer close(output.respChan)
	defer close(output.retryChan)
	inChan := runner.InChan()
	ok := true

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				return
			}
			outgoing, err = output.runner.Encode(pack)
			if err != nil {
				return
			}

			err = output.sendMessage(outgoing)
			if err != nil {
				runner.LogError(err)
				msg := RetryMsg{body: outgoing, retryChan: output.retryChan, maxCount: 3}
				err = msg.Retry()
				if err != nil {
					runner.LogError(fmt.Errorf("%s. No longer attempting to send message.", err.Error()))
				}
			}
			pack.Recycle()
		case msg := <-output.retryChan:
			err = output.sendMessage(msg.body)
			if err != nil {
				runner.LogError(err)
				err = msg.Retry()
				if err != nil {
					runner.LogError(fmt.Errorf("%s. No longer attempting to send message.", err.Error()))
				}
			}
		}
	}

	for _, producer := range output.producers {
		producer.Stop()
	}

	return nil
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
	return
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
			msg := RetryMsg{body: body, retryChan: output.retryChan, maxCount: 3}
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
