package nsq

import (
	"errors"
	"fmt"
	"github.com/bitly/go-hostpool"
	"github.com/bitly/go-nsq"
	"github.com/mozilla-services/heka/pipeline"
	"sync/atomic"
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

	var pack *pipeline.PipelinePack

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
			err = output.sendMessage(pack)
			if err != nil {
				runner.LogError(err)
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
func (output *NsqOutput) sendMessage(pack *pipeline.PipelinePack) (err error) {
	var body []byte
	body, err = output.runner.Encode(pack)
	if err != nil {
		return
	}

	switch output.Mode {
	case ModeRoundRobin:
		counter := atomic.AddUint64(&output.counter, 1)
		idx := counter % uint64(len(output.Addresses))
		addr := output.Addresses[idx]
		p := output.producers[addr]
		err = p.PublishAsync(output.Topic, body, output.respChan, pack)
	case ModeHostPool:
		hostPoolResponse := output.hostPool.Get()
		p := output.producers[hostPoolResponse.Host()]
		err = p.PublishAsync(output.Topic, body, output.respChan, pack, hostPoolResponse)
	}
	return
}

// responder handles the eventual response from the asyncronous publish. If
// there was an error when publishing, then we put the pipeline pack back onto
// outputs inChan so that it can be republished. If there was no error, then the
// pack gets recycled.
func (output *NsqOutput) responder() {
	var (
		pack             *pipeline.PipelinePack
		hostPoolResponse hostpool.HostPoolResponse
		inChan           chan *pipeline.PipelinePack
	)

	for t := range output.respChan {
		switch output.Mode {
		case ModeRoundRobin:
			pack = t.Args[0].(*pipeline.PipelinePack)
		case ModeHostPool:
			pack = t.Args[0].(*pipeline.PipelinePack)
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

		// On success we recycle the pack, if it failed put it back into the
		// recycle chan to be redelivered
		if success {
			pack.Recycle()
		} else {
			output.runner.LogError(fmt.Errorf("Publishing message failed: %s", t.Error.Error()))
			if inChan == nil {
				inChan = output.runner.InChan()
			}
			inChan <- pack
		}
	}
}

func init() {
	pipeline.RegisterPlugin("NsqOutput", func() interface{} {
		return new(NsqOutput)
	})
}
