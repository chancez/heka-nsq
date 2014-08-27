package nsq

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/mozilla-services/heka/pipeline"
	pipeline_ts "github.com/mozilla-services/heka/pipeline/testsupport"
	"github.com/mozilla-services/heka/pipelinemock"
	"github.com/mozilla-services/heka/plugins"
	plugins_ts "github.com/mozilla-services/heka/plugins/testsupport"
	"github.com/rafrombrc/gomock/gomock"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"sync"
)

func NsqInputSpec(c gs.Context) {
	t := new(pipeline_ts.SimpleT)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pConfig := pipeline.NewPipelineConfig(nil)
	var wg sync.WaitGroup

	errChan := make(chan error, 1)
	retPackChan := make(chan *pipeline.PipelinePack, 1)
	defer close(errChan)
	defer close(retPackChan)

	c.Specify("A nsq input", func() {
		input := new(NsqInput)
		ith := new(plugins_ts.InputTestHelper)
		ith.MockHelper = pipelinemock.NewMockPluginHelper(ctrl)
		ith.MockInputRunner = pipelinemock.NewMockInputRunner(ctrl)

		config := input.ConfigStruct().(*NsqInputConfig)
		config.Topic = "test_topic"
		config.Channel = "test_channel"

		startInput := func() {
			wg.Add(1)
			go func() {
				errChan <- input.Run(ith.MockInputRunner, ith.MockHelper)
				wg.Done()
			}()
		}

		var mockConsumer *MockConsumer
		input.newConsumer = func(topic, channel string, config *nsq.Config) (Consumer, error) {
			mockConsumer, _ = NewMockConsumer(topic, channel, config)
			return mockConsumer, nil
		}

		ith.Pack = pipeline.NewPipelinePack(pConfig.InputRecycleChan())
		ith.PackSupply = make(chan *pipeline.PipelinePack, 1)
		ith.PackSupply <- ith.Pack

		inputName := "NsqInput"
		ith.MockInputRunner.EXPECT().Name().Return(inputName).AnyTimes()
		ith.MockInputRunner.EXPECT().InChan().Return(ith.PackSupply)

		c.Specify("that is started", func() {
			input.SetPipelineConfig(pConfig)

			c.Specify("gets messages its subscribed to", func() {
				c.Specify("injects messages into the pipeline when not configured with a decoder", func() {
					ith.MockInputRunner.EXPECT().Inject(ith.Pack).Do(func(p *pipeline.PipelinePack) {
						retPackChan <- p
					}).AnyTimes()

					err := input.Init(config)
					c.Expect(err, gs.IsNil)

					c.Expect(input.DecoderName, gs.Equals, "")

				})

				c.Specify("injects messages into the decoder when configured", func() {
					decoderName := "ScribbleDecoder"
					config.DecoderName = decoderName
					decoder := new(plugins.ScribbleDecoder)
					decoder.Init(&plugins.ScribbleDecoderConfig{})

					mockDecoderRunner := pipelinemock.NewMockDecoderRunner(ctrl)
					mockDecoderRunner.EXPECT().Decoder().Return(decoder)
					mockDecoderRunner.EXPECT().InChan().Return(retPackChan)
					ith.MockHelper.EXPECT().DecoderRunner(decoderName,
						fmt.Sprintf("%s-%s", inputName, decoderName),
					).Return(mockDecoderRunner, true)

					err := input.Init(config)
					c.Expect(err, gs.IsNil)

					c.Expect(input.DecoderName, gs.Equals, decoderName)
				})

				startInput()
				// Should get two finished conn calls since we connect
				// to lookupds and nsqds
				c.Expect(<-mockConsumer.finishedConn, gs.IsTrue)
				c.Expect(<-mockConsumer.finishedConn, gs.IsTrue)

				id := nsq.MessageID{}
				body := []byte("test")
				msg := nsq.NewMessage(id, body)
				for _, handler := range mockConsumer.handlers {
					handler.HandleMessage(msg)
				}

				p := <-retPackChan
				c.Expect(p.Message.GetPayload(), gs.Equals, "test")

			})

			input.Stop()
			wg.Wait()
			c.Expect(<-errChan, gs.IsNil)
		})

	})

}
