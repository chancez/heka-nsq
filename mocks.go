package nsq

import (
	"github.com/bitly/go-nsq"
)

type MockProducer struct {
	respChan chan *nsq.ProducerTransaction
}

type MockConsumer struct {
	stopChan     chan int
	handlers     []nsq.Handler
	finishedConn chan bool
}

func NewMockProducer(addr string, config *nsq.Config) (*MockProducer, error) {
	return &MockProducer{respChan: make(chan *nsq.ProducerTransaction, 1)}, nil
}

func (p *MockProducer) PublishAsync(topic string, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	transaction := &nsq.ProducerTransaction{Error: nil, Args: args}
	doneChan <- transaction
	p.respChan <- transaction
	return nil
}

func (p *MockProducer) Stop() {
	close(p.respChan)
}

func NewMockConsumer(topic, channel string, config *nsq.Config) (*MockConsumer, error) {
	consumer := &MockConsumer{
		stopChan:     make(chan int, 1),
		handlers:     make([]nsq.Handler, 0),
		finishedConn: make(chan bool, 2),
	}
	return consumer, nil
}

func (p *MockProducer) SetLogger(l nsq.Logger, lvl nsq.LogLevel) {
}

func (c *MockConsumer) AddHandler(handler nsq.Handler) {
	c.handlers = append(c.handlers, handler)
}

func (c *MockConsumer) ConnectToNSQDs(addresses []string) error {
	c.finishedConn <- true
	return nil
}

func (c *MockConsumer) ConnectToNSQLookupds(addresses []string) error {
	c.finishedConn <- true
	return nil
}

func (c *MockConsumer) StoppedChan() chan int {
	return c.stopChan
}

func (c *MockConsumer) Stop() {
	close(c.stopChan)
}

func (c *MockConsumer) SetLogger(l nsq.Logger, lvl nsq.LogLevel) {
}
