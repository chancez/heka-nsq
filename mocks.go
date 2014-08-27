package nsq

import (
	"github.com/bitly/go-nsq"
)

type MockProducer struct {
	respChan chan *nsq.ProducerTransaction
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

func (p *MockProducer) SetLogger(l nsq.Logger, lvl nsq.LogLevel) {
}
