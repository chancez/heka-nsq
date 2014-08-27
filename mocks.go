package nsq

import (
	"github.com/bitly/go-nsq"
)

type MockProducer struct {
}

func NewMockProducer(addr string, config *nsq.Config) (Producer, error) {
	return &MockProducer{}, nil
}

func (p *MockProducer) PublishAsync(topic string, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	transaction := &nsq.ProducerTransaction{Error: nil, Args: args}
	doneChan <- transaction
	return nil
}

func (p *MockProducer) Stop() {
}

func (p *MockProducer) SetLogger(l nsq.Logger, lvl nsq.LogLevel) {
}
