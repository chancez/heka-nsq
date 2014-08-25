package nsq

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/plugins/tcp"
	"time"
)

type NsqInputConfig struct {
	LookupdAddrs []string `toml:"lookupd_http_addresses"`
	NsqdAddrs    []string `toml:"nsqd_tcp_addresses"`
	Topic        string   `toml:"topic"`
	Channel      string   `toml:"channel"`
	DecoderName  string   `toml:"decoder"`
	UseMsgBytes  *bool    `toml:"use_msgbytes"`

	// Set to true if the TCP connection should be tunneled through TLS.
	// Requires additional Tls config section.
	UseTls bool `toml:"use_tls"`
	// Subsection for TLS configuration.
	Tls tcp.TlsConfig

	// The following fields config options for nsq.Config

	// Deadlines for network reads and writes
	ReadTimeout  *int64 `toml:"read_timeout"`
	WriteTimeout *int64 `toml:"write_timeout"`

	// Duration between polling lookupd for new producers, and fractional jitter to add to
	// the lookupd pool loop. this helps evenly distribute requests even if multiple consumers
	// restart at the same time
	LookupdPollInterval *int64   `toml:"lookupd_poll_interval"`
	LookupdPollJitter   *float64 `toml:"lookupd_poll_jitter"`

	// Maximum duration when REQueueing (for doubling of deferred requeue)
	MaxRequeueDelay     *int64 `toml:"max_requeue_delay"`
	DefaultRequeueDelay *int64 `toml:"default_requeue_delay"`
	// Unit of time for calculating consumer backoff
	BackoffMultiplier *int64 `toml:"backoff_multiplier"`

	// Maximum number of times this consumer will attempt to process a message before giving up
	MaxAttempts *uint16 `toml:"max_attempts"`
	// Amount of time in seconds to wait for a message from a producer when in a state where RDY
	// counts are re-distributed (ie. max_in_flight < num_producers)
	LowRdyIdleTimeout *int64 `toml:"low_rdy_idle_timeout"`

	// Identifiers sent to nsqd representing this client
	// UserAgent is in the spirit of HTTP (default: "<client_library_name>/<version>")
	ClientID  *string `toml:"client_id"` // (defaults: short hostname)
	Hostname  *string `toml:"hostname"`
	UserAgent *string `toml:"user_agent"`

	// Duration of time between heartbeats. This must be less than ReadTimeout
	HeartbeatInterval *int64 `toml:"heartbeat_interval"`
	// Integer percentage to sample the channel (requires nsqd 0.2.25+)
	SampleRate *int32 `toml:"sample_rate"`

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

type NsqInput struct {
	*NsqInputConfig
	consumer    *nsq.Consumer
	config      *nsq.Config
	decoderChan chan *pipeline.PipelinePack
	runner      pipeline.InputRunner
	pConfig     *pipeline.PipelineConfig
	packSupply  chan *pipeline.PipelinePack
}

func (input *NsqInput) ConfigStruct() interface{} {
	return &NsqInputConfig{}
}

func (input *NsqInput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	input.pConfig = pConfig
}

func (input *NsqInput) SetNsqConfig(conf *NsqInputConfig) (err error) {
	nsqConfig := nsq.NewConfig()

	if conf.LookupdPollInterval != nil {
		nsqConfig.LookupdPollInterval = time.Duration(*conf.LookupdPollInterval) * time.Millisecond
	}
	if conf.LookupdPollJitter != nil {
		nsqConfig.LookupdPollJitter = *conf.LookupdPollJitter
	}
	if conf.MaxRequeueDelay != nil {
		nsqConfig.MaxRequeueDelay = time.Duration(*conf.MaxRequeueDelay) * time.Millisecond
	}
	if conf.DefaultRequeueDelay != nil {
		nsqConfig.DefaultRequeueDelay = time.Duration(*conf.DefaultRequeueDelay) * time.Millisecond
	}
	if conf.BackoffMultiplier != nil {
		nsqConfig.BackoffMultiplier = time.Duration(*conf.BackoffMultiplier) * time.Millisecond
	}
	if conf.MaxAttempts != nil {
		nsqConfig.MaxAttempts = *conf.MaxAttempts
	}
	if conf.LowRdyIdleTimeout != nil {
		nsqConfig.LowRdyIdleTimeout = time.Duration(*conf.LowRdyIdleTimeout) * time.Millisecond
	}
	if conf.SampleRate != nil {
		nsqConfig.SampleRate = *conf.SampleRate
	}

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
		input.config.TlsConfig = tls
	}
	input.config = nsqConfig

	return
}

func (input *NsqInput) Init(config interface{}) (err error) {
	conf := config.(*NsqInputConfig)
	input.NsqInputConfig = conf
	err = input.SetNsqConfig(conf)
	if err != nil {
		return
	}
	input.consumer, err = nsq.NewConsumer(input.Topic, input.Channel, input.config)
	input.consumer.SetLogger(nil, nsq.LogLevelError)

	return nil
}

func (input *NsqInput) Run(runner pipeline.InputRunner,
	helper pipeline.PluginHelper) (err error) {
	var (
		dRunner     pipeline.DecoderRunner
		ok          bool
		useMsgBytes bool
	)

	if input.DecoderName != "" {
		if dRunner, ok = input.pConfig.DecoderRunner(input.DecoderName,
			fmt.Sprintf("%s-%s", runner.Name(), input.DecoderName)); !ok {
			return fmt.Errorf("Decoder not found: %s", input.DecoderName)
		}
		input.decoderChan = dRunner.InChan()
	}

	if input.UseMsgBytes == nil {
		// Only override if not already set
		if dRunner != nil {
			// We want to know what kind of decoder is being used, but we only
			// care if they're using a protobuf decoder, or a multidecoder with
			// a protobuf decoder as the first sub decoder
			decoder := dRunner.Decoder()
			switch decoder.(type) {
			case *pipeline.ProtobufDecoder:
				useMsgBytes = true
			case *pipeline.MultiDecoder:
				d := decoder.(*pipeline.MultiDecoder)
				if len(d.Decoders) > 0 {
					if _, ok := d.Decoders[0].(*pipeline.ProtobufDecoder); ok {
						useMsgBytes = true
					}
				}
			}
		}
		input.UseMsgBytes = &useMsgBytes
	}

	input.runner = runner
	input.packSupply = runner.InChan()

	input.consumer.AddHandler(input)

	err = input.consumer.ConnectToNSQDs(input.NsqdAddrs)
	if err != nil {
		return err
	}
	err = input.consumer.ConnectToNSQLookupds(input.LookupdAddrs)
	if err != nil {
		return err
	}

	<-input.consumer.StopChan

	return nil
}

func (input *NsqInput) Stop() {
	input.consumer.Stop()
}

func (input *NsqInput) HandleMessage(msg *nsq.Message) error {
	pack := <-input.packSupply
	// If we're using protobuf, then the entire message is in the
	// nats message body
	if *input.UseMsgBytes {
		pack.MsgBytes = msg.Body
	} else {
		pack.Message.SetUuid(uuid.NewRandom())
		pack.Message.SetTimestamp(time.Now().UnixNano())
		pack.Message.SetType("nsq.input")
		pack.Message.SetHostname(input.pConfig.Hostname())
		pack.Message.SetPayload(string(msg.Body))
		message.NewStringField(pack.Message, "topic", input.Topic)
		message.NewStringField(pack.Message, "channel", input.Channel)
	}
	input.sendPack(pack)
	return nil
}

func (input *NsqInput) sendPack(pack *pipeline.PipelinePack) {
	if input.decoderChan != nil {
		input.decoderChan <- pack
	} else {
		input.runner.Inject(pack)
	}
}

func init() {
	pipeline.RegisterPlugin("NsqInput", func() interface{} {
		return new(NsqInput)
	})
}
