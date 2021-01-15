package nats

import (
	"context"
	"encoding/json"
	"log"
	"reflect"

	"github.com/nats-io/nats.go"
	"github.com/sei-ri/pigeon"
	"github.com/sei-ri/pigeon/internal"
)

type Broker struct {
	conn         *nats.Conn
	url          string
	options      []nats.Option
	debugEnabled bool
}

type Option func(c *Broker) error

func WithURL(v string) Option {
	return func(c *Broker) error {
		c.url = v
		return nil
	}
}

func WithOptions(v ...nats.Option) Option {
	return func(c *Broker) error {
		c.options = v
		return nil
	}
}

func WithDebug(v bool) Option {
	return func(c *Broker) error {
		c.debugEnabled = v
		return nil
	}
}

func New(ctx context.Context, opts ...Option) (*Broker, error) {
	b := &Broker{
		url:     nats.DefaultURL,
		options: []nats.Option{},
	}

	for i := range opts {
		if err := opts[i](b); err != nil {
			return nil, err
		}
	}

	return b, b.open(ctx)
}

func (b *Broker) open(ctx context.Context) error {
	nc, err := nats.Connect(b.url, b.options...)
	if err != nil {
		return err
	}
	b.conn = nc
	log.Println("[PIGEON] nats connected at", b.url)
	return nil
}

func (b *Broker) Close() error {
	if b.conn != nil {
		return b.conn.Drain()
	}
	return nil
}

func (b *Broker) Publish(event pigeon.Event) error {
	raw, err := json.Marshal(event.Data)
	if err != nil {
		return err
	}

	subj := internal.ParseType(event.Data).String()

	if b.debugEnabled {
		log.Println("[PIGEON] publish: ", subj)
	}

	b.conn.Publish(subj, raw)
	b.conn.Flush()

	return b.conn.LastError()
}

func (b *Broker) Subscribe(typ reflect.Type, processor pigeon.EventProcessor) error {
	if b.debugEnabled {
		log.Println("[PIGEON] subscribe: ", typ.String())
	}

	_, err := b.conn.Subscribe(typ.String(), func(msg *nats.Msg) {
		elem := reflect.New(typ).Interface()
		if err := json.Unmarshal(msg.Data, elem); err != nil {
			log.Println("[PIGEON] json unmarshal: %v", err)
		}
		if err := processor(pigeon.Event{Data: elem}); err != nil {
			log.Println("[PIGEON] failed to event processor: %v", err)
		}
	})

	return err
}
