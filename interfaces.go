package nsq

import (
	"github.com/bitly/go-nsq"
)

type Producer interface {
	PublishAsync(topic string, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	Stop()
	SetLogger(l nsq.Logger, lvl nsq.LogLevel)
}

type Consumer interface {
	SetLogger(l nsq.Logger, lvl nsq.LogLevel)
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
	return nsq.NewProducer(addr, config)
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
