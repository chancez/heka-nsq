package nsq

import (
	"errors"
	"github.com/bitly/go-nsq"
	"github.com/mozilla-services/heka/pipeline"
)

type NsqOutputConfig struct {
	Address string `toml:"nsqd_address"`
	Topic   string `toml:"topic"`
}

type NsqOutput struct {
	*NsqOutputConfig
	producer *nsq.Producer
	config   *nsq.Config
}

func (output *NsqOutput) ConfigStruct() interface{} {
	return &NsqOutputConfig{}
}

func (output *NsqOutput) Init(config interface{}) error {
	conf := config.(*NsqOutputConfig)
	output.NsqOutputConfig = conf
	output.config = nsq.NewConfig()
	return nil
}

func (output *NsqOutput) Run(runner pipeline.OutputRunner,
	helper pipeline.PluginHelper) (err error) {
	if runner.Encoder() == nil {
		return errors.New("Encoder required.")
	}
	output.producer, err = nsq.NewProducer(output.Address, output.config)
	if err != nil {
		return
	}
	defer output.producer.Stop()

	var (
		pack     *pipeline.PipelinePack
		outgoing []byte
	)

	inChan := runner.InChan()
	ok := true

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				return
			}
			outgoing, err = runner.Encode(pack)
			if err != nil {
				runner.LogError(err)
				continue
			}
			err = output.producer.Publish(output.Topic, outgoing)
			if err != nil {
				runner.LogError(err)
			}
			pack.Recycle()
		}
	}

	return nil
}

func init() {
	pipeline.RegisterPlugin("NsqOutput", func() interface{} {
		return new(NsqOutput)
	})
}
