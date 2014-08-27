package nsq

import (
	"github.com/bitly/go-nsq"
)

type Producer interface {
	PublishAsync(topic string, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	Stop()
	SetLogger(l Logger, lvl nsq.LogLevel)
}

type NsqProducer struct {
	*nsq.Producer
}

type Logger interface {
	Output(calldepth int, s string) error
}

type Consumer interface {
	SetLogger(l Logger, lvl nsq.LogLevel)
	AddHandler(handler nsq.Handler)
	ConnectToNSQDs(addresses []string) error
	ConnectToNSQLookupds(addresses []string) error
	StoppedChan() chan int
	Stop()
}

// NsqConsumer is a wrapper around nsq.Consumer which provides implements the
// Consumer interface.
type NsqConsumer struct {
	*nsq.Consumer
}

func NewProducer(addr string, config *nsq.Config) (Producer, error) {
	producer, err := nsq.NewProducer(addr, config)
	if err != nil {
		return nil, err
	}
	return &NsqProducer{Producer: producer}, nil
}

func (p *NsqProducer) SetLogger(l Logger, lvl nsq.LogLevel) {
	p.Producer.SetLogger(l, lvl)
}

func NewConsumer(topic string, channel string, config *nsq.Config) (Consumer, error) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return nil, err
	}
	return &NsqConsumer{Consumer: consumer}, nil
}

func (c *NsqConsumer) StoppedChan() chan int {
	return c.StopChan
}

func (c *NsqConsumer) SetLogger(l Logger, lvl nsq.LogLevel) {
	c.Consumer.SetLogger(l, lvl)
}
