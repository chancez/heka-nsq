package nsq

import (
	"errors"
	"fmt"
	"github.com/bitly/go-hostpool"
	"github.com/bitly/go-nsq"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/plugins/tcp"
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
	RetryQueueSize uint64   `toml:"retry_queue_size"`
	MaxMsgRetries  uint64   `toml:"max_msg_retries"`
	RetryOptions   *pipeline.RetryOptions

	// Set to true if the TCP connection should be tunneled through TLS.
	// Requires additional Tls config section.
	UseTls bool `toml:"use_tls"`
	// Subsection for TLS configuration.
	Tls tcp.TlsConfig

	// The following fields config options for nsq.Config

	// Deadlines for network reads and writes
	ReadTimeout  *int64 `toml:"read_timeout"`
	WriteTimeout *int64 `toml:"write_timeout"`

	// Identifiers sent to nsqd representing this client
	// UserAgent is in the spirit of HTTP (default: "<client_library_name>/<version>")
	ClientID  *string `toml:"client_id"` // (defaults: short hostname)
	Hostname  *string `toml:"hostname"`
	UserAgent *string `toml:"user_agent"`

	// Duration of time between heartbeats. This must be less than ReadTimeout
	HeartbeatInterval *int64 `toml:"heartbeat_interval"`
	// Compression Settings
	Deflate      *bool `toml:"deflate"`
	DeflateLevel *int  `toml:"deflate_level"`
	Snappy       *bool `toml:"snappy"`

	// Size of the buffer (in bytes) used by nsqd for buffering writes to this connection
	OutputBufferSize *int64 `toml:"output_buffer_size"`
	// Timeout used by nsqd before flushing buffered writes (set to 0 to disable).
	//
	// WARNING: configuring clients with an extremely low
	// (< 25ms) output_buffer_timeout has a significant effect
	// on nsqd CPU usage (particularly with > 50 clients connected).
	OutputBufferTimeout *int64 `toml:"output_buffer_timeout"`

	// Maximum number of messages to allow in flight (concurrency knob)
	MaxInFlight *int `toml:"max_in_flight"`

	// Maximum amount of time to backoff when processing fails 0 == no backoff
	MaxBackoffDuration *int64 `toml:"max_backoff_duration"`

	// The server-side message timeout for messages delivered to this client
	MsgTimeout *int64 `toml:"msg_timeout"`

	// secret for nsqd authentication (requires nsqd 0.2.29+)
	AuthSecret *string `toml:"auth_secret"`
}

type NsqOutput struct {
	*NsqOutputConfig
	Mode        int
	runner      pipeline.OutputRunner
	producers   map[string]Producer
	config      *nsq.Config
	respChan    chan *nsq.ProducerTransaction
	counter     uint64
	hostPool    hostpool.HostPool
	retryChan   chan RetryMsg
	retryHelper *pipeline.RetryHelper
	newProducer func(string, *nsq.Config) (Producer, error)
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
		RetryQueueSize: 50,
		RetryOptions: &pipeline.RetryOptions{
			MaxRetries: 3,
			Delay:      "1s",
		},
	}
}

func (output *NsqOutput) SetNsqConfig(conf *NsqOutputConfig) (err error) {
	nsqConfig := nsq.NewConfig()
	if conf.ReadTimeout != nil {
		nsqConfig.ReadTimeout = time.Duration(*conf.ReadTimeout) * time.Millisecond
	}
	if conf.WriteTimeout != nil {
		nsqConfig.WriteTimeout = time.Duration(*conf.WriteTimeout) * time.Millisecond
	}
	if conf.ClientID != nil {
		nsqConfig.ClientID = *conf.ClientID
	}
	if conf.Hostname != nil {
		nsqConfig.Hostname = *conf.Hostname
	}
	if conf.UserAgent != nil {
		nsqConfig.UserAgent = *conf.UserAgent
	}
	if conf.HeartbeatInterval != nil {
		nsqConfig.HeartbeatInterval = time.Duration(*conf.HeartbeatInterval) * time.Millisecond
	}
	if conf.Deflate != nil {
		nsqConfig.Deflate = *conf.Deflate
	}
	if conf.DeflateLevel != nil {
		nsqConfig.DeflateLevel = *conf.DeflateLevel
	}
	if conf.Snappy != nil {
		nsqConfig.Snappy = *conf.Snappy
	}
	if conf.OutputBufferSize != nil {
		nsqConfig.OutputBufferSize = *conf.OutputBufferSize
	}
	if conf.OutputBufferTimeout != nil {
		nsqConfig.OutputBufferTimeout = time.Duration(*conf.OutputBufferTimeout) * time.Millisecond
	}
	if conf.MaxInFlight != nil {
		nsqConfig.MaxInFlight = *conf.MaxInFlight
	}
	if conf.MaxBackoffDuration != nil {
		nsqConfig.MaxBackoffDuration = time.Duration(*conf.MaxBackoffDuration) * time.Millisecond
	}
	if conf.MsgTimeout != nil {
		nsqConfig.MsgTimeout = time.Duration(*conf.MsgTimeout) * time.Millisecond
	}
	if conf.AuthSecret != nil {
		nsqConfig.AuthSecret = *conf.AuthSecret
	}

	// Tls
	if conf.UseTls {
		tls, err := tcp.CreateGoTlsConfig(&conf.Tls)
		if err != nil {
			return err
		}
		output.config.TlsConfig = tls
	}
	output.config = nsqConfig

	return
}

func (output *NsqOutput) Init(config interface{}) (err error) {
	conf := config.(*NsqOutputConfig)
	output.NsqOutputConfig = conf

	if len(conf.Addresses) < 1 {
		return errors.New("Need at least one nsqd address.")
	}

	err = output.SetNsqConfig(conf)
	if err != nil {
		return
	}

	switch conf.Mode {
	case "round-robin":
		output.Mode = ModeRoundRobin
	case "hostpool":
		output.Mode = ModeHostPool
	}

	if output.newProducer == nil {
		output.newProducer = NewProducer
	}

	output.producers = make(map[string]Producer)
	var producer Producer
	for _, addr := range output.Addresses {
		producer, err = output.newProducer(addr, output.config)
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

	output.retryHelper, err = pipeline.NewRetryHelper(*output.RetryOptions)
	if err != nil {
		return
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
		msg      RetryMsg
	)

	output.runner = runner
	inChan := runner.InChan()
	ok := true

	defer output.cleanup()

	for ok {
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
					output.runner.LogError(err)
					err = output.retryHelper.Wait()
					if err != nil {
						return
					}
					// Create a retry msg, and requeue it
					msg := RetryMsg{Body: outgoing, retryChan: output.retryChan, maxCount: output.MaxMsgRetries}
					err = msg.Retry()
					if err != nil {
						output.runner.LogError(err)
					}
				} else {
					output.retryHelper.Reset()
				}
			}
			pack.Recycle()
		case msg, ok = <-output.retryChan:
			if !ok {
				return nil
			}
			err = output.sendMessage(msg.Body)
			if err != nil {
				output.runner.LogError(err)
				err = output.retryHelper.Wait()
				if err != nil {
					return
				}
				// requeue the message
				err = msg.Retry()
				if err != nil {
					output.runner.LogError(err)
				}
			} else {
				output.retryHelper.Reset()
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
	return
}

// responder handles the eventual response from the asyncronous publish.
// It handles retrying to send the message if there was an error
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
