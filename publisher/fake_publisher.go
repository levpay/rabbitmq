package publisher

import (
	"fmt"

	"github.com/nuveo/log"
)

type PublisherMock struct{}

func (p *PublisherMock) publishWithoutRetry(d *Declare) (err error)           { return }
func (p *PublisherMock) PublishWithDelay(d *Declare, delay int64) (err error) { return }
func (p *PublisherMock) createExchangeAndQueueDLX(d *Declare) (err error)     { return }
func (p *PublisherMock) Publish(d *Declare) (err error) {
	fmt.Printf("MOCK >>>>> %+v\n\n", string(d.Body))
	return
}

func NewFakePublisher() (p *PublisherMock, err error) {
	log.Println("New Publisher Mock...")
	p = &PublisherMock{}
	return
}
