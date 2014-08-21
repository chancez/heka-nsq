package nsq

import (
	"errors"
	"fmt"
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
}

func (output *NsqOutput) ConfigStruct() interface{} {
	return &NsqOutputConfig{}
}

func (output *NsqOutput) Init(config interface{}) (err error) {
	conf := config.(*NsqOutputConfig)
	output.NsqOutputConfig = conf
	output.config = nsq.NewConfig()
	output.producers = make(map[string]*nsq.Producer)

	switch conf.Mode {
	case "round-robin":
		output.Mode = ModeRoundRobin
	case "hostpool":
		output.Mode = ModeHostPool
	}

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
			startTime := time.Now()
			switch output.Mode {
			case ModeRoundRobin:
				counter := atomic.AddUint64(&output.counter, 1)
				idx := counter % uint64(len(output.Addresses))
				addr := output.Addresses[idx]
				producer := output.producers[addr]
				err = producer.PublishAsync(output.Topic, outgoing, output.respChan, pack, startTime, addr)
			}
			if err != nil {
				runner.LogError(err)
			}
			pack.Recycle()
		}
	}

	for _, producer := range output.producers {
		producer.Stop()
	}

	return nil
}

func (output *NsqOutput) responder() {
	var (
		pack *pipeline.PipelinePack
		// startTime time.Time
		// address   string
	)
	for t := range output.respChan {
		switch output.Mode {
		case ModeRoundRobin:
			pack = t.Args[0].(*pipeline.PipelinePack)
			// startTime = t.Args[1].(time.Time)
			// address = t.Args[2].(string)
		}

		if t.Error != nil {
			output.runner.LogError(fmt.Errorf("Publishing message failed: %s", t.Error.Error()))
			output.runner.InChan() <- pack
		}
	}
}

func init() {
	pipeline.RegisterPlugin("NsqOutput", func() interface{} {
		return new(NsqOutput)
	})
}
