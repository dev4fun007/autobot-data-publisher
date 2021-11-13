package publisher

import (
	"context"
	"github.com/dev4fun007/autobot-common"
	"github.com/rs/zerolog/log"
	"strconv"
)

const (
	QueueBufferSize  = 8
	DataPublisherTag = "TickerDataPublisher"
)

type TickerDataPublisher struct {
	registry        common.WorkerRegistryService // get all active worker data channel to send data to
	marketWorkerMap map[string]common.Worker
	tickerDataChan  chan []common.TickerData // use this READ-ONLY channel to read the ticker data received from broker
}

func NewDataPublisher(registry common.WorkerRegistryService) TickerDataPublisher {
	return TickerDataPublisher{
		registry:       registry,
		tickerDataChan: make(chan []common.TickerData, QueueBufferSize),
	}
}

func (receiver TickerDataPublisher) Publish(tickers []common.TickerData) {
	receiver.tickerDataChan <- tickers
}

func (receiver TickerDataPublisher) StartFanOutService(ctx context.Context) {
	log.Info().Str(common.LogComponent, DataPublisherTag).
		Msg("starting data fan out service")
	go func() {
		for {
			select {
			case data := <-receiver.tickerDataChan:
				{
					marketWorkerMap := receiver.registry.GetActiveWorkers()
					log.Debug().Str(common.LogComponent, DataPublisherTag).Int("worker-count", len(marketWorkerMap)).Msg("fetched all active workers")
					for _, tickerData := range data {
						if val, ok := marketWorkerMap[tickerData.Market]; ok {
							for _, worker := range val {
								value, err := strconv.ParseFloat(tickerData.LastPrice, 64)
								if err != nil {
									log.Error().Str(common.LogComponent, DataPublisherTag).Err(err).Msg("unable to parse ticker lastPrice to float64")
									break
								}
								tickerData.LastPriceCalculated = value
								worker.GetDataChannel() <- tickerData
							}
						}
					}
				}
			case <-ctx.Done():
				{
					log.Info().Str(common.LogComponent, DataPublisherTag).
						Msg("stopping data listener and publisher")
					return
				}
			}
		}
	}()
}
