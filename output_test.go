package nsq

import (
	"github.com/bitly/go-nsq"
	"github.com/mozilla-services/heka/pipeline"
	pipeline_ts "github.com/mozilla-services/heka/pipeline/testsupport"
	"github.com/mozilla-services/heka/plugins"
	plugins_ts "github.com/mozilla-services/heka/plugins/testsupport"
	"github.com/rafrombrc/gomock/gomock"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"sync"
)

func NsqOutputSpec(c gs.Context) {
	t := new(pipeline_ts.SimpleT)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pConfig := pipeline.NewPipelineConfig(nil)
	var wg sync.WaitGroup

	errChan := make(chan error, 1)
	defer close(errChan)

	c.Specify("A nsq output", func() {
		output := new(NsqOutput)
		oth := plugins_ts.NewOutputTestHelper(ctrl)
		config := output.ConfigStruct().(*NsqOutputConfig)
		config.Topic = "test"

		startOutput := func() {
			wg.Add(1)
			go func() {
				errChan <- output.Run(oth.MockOutputRunner, oth.MockHelper)
				wg.Done()
			}()
		}

		var mockProducer *MockProducer
		output.newProducer = func(addr string, config *nsq.Config) (Producer, error) {
			mockProducer, _ = NewMockProducer(addr, config)
			return mockProducer, nil
		}

		msg := pipeline_ts.GetTestMessage()
		pack := pipeline.NewPipelinePack(pConfig.InputRecycleChan())
		pack.Message = msg

		c.Specify("requires at least one address", func() {
			err := output.Init(config)
			c.Expect(err, gs.Not(gs.IsNil))
		})

		config.Addresses = []string{"test.com:4150"}

		c.Specify("requires an encoder", func() {
			err := output.Init(config)
			c.Assume(err, gs.IsNil)

			oth.MockOutputRunner.EXPECT().Encoder().Return(nil)
			err = output.Run(oth.MockOutputRunner, oth.MockHelper)
			c.Expect(err, gs.Not(gs.IsNil))
		})

		c.Specify("that is started", func() {
			encoder := new(plugins.PayloadEncoder)
			encoder.Init(&plugins.PayloadEncoderConfig{PrefixTs: false})
			oth.MockOutputRunner.EXPECT().Encoder().Return(encoder)
			oth.MockOutputRunner.EXPECT().Encode(pack).Return(encoder.Encode(pack))

			inChan := make(chan *pipeline.PipelinePack, 1)
			oth.MockOutputRunner.EXPECT().InChan().Return(inChan)

			c.Specify("publishes a message with the configured topic", func() {
				err := output.Init(config)
				c.Assume(err, gs.IsNil)

				startOutput()

				inChan <- pack
				transaction := <-mockProducer.respChan
				c.Expect(transaction.Error, gs.IsNil)

				close(inChan)
				wg.Wait()
				c.Expect(<-errChan, gs.IsNil)
			})
		})

	})
}
