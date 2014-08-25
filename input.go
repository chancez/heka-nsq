package nsq

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"time"
)

type NsqInputConfig struct {
	LookupdAddrs []string `toml:"lookupd_http_addresses"`
	NsqdAddrs    []string `toml:"nsqd_tcp_addresses"`
	Topic        string   `toml:"topic"`
	Channel      string   `toml:"channel"`
	DecoderName  string   `toml:"decoder"`
	UseMsgBytes  *bool    `toml:"use_msgbytes"`
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

func (input *NsqInput) Init(config interface{}) (err error) {
	conf := config.(*NsqInputConfig)
	input.NsqInputConfig = conf
	input.config = nsq.NewConfig()
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
