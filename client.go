package pigeon

import (
	"context"
	"log"

	"github.com/sei-ri/pigeon/internal"
)

type Client struct {
	aggregate Aggregate
	commands  map[string]CommandHandler
	events    map[string][]EventProcessor
	types     Types
	broker    Broker
	storage   Storage
	develop   bool
}

type Option func(c *Client) error

func WithStorage(v Storage) Option {
	return func(c *Client) error {
		c.storage = v
		return nil
	}
}

func WithBroker(v Broker) Option {
	return func(c *Client) error {
		c.broker = v
		return nil
	}
}

func WithDevelop(v bool) Option {
	return func(c *Client) error {
		c.develop = v
		return nil
	}
}

func NewClient(ctx context.Context, aggregate Aggregate, opts ...Option) (*Client, error) {
	c := &Client{
		aggregate: aggregate,
		commands:  map[string]CommandHandler{},
		events:    map[string][]EventProcessor{},
		types:     newTypes(),
		storage:   newStorage(),
		develop:   true,
	}

	for i := range opts {
		if err := opts[i](c); err != nil {
			return nil, err
		}
	}

	c.broker = newBroker(c.develop)

	return c, nil
}

func (c *Client) AddCommands(args ...Command) error {
	for i := range args {
		k := internal.ParseType(args[i]).String()

		if _, ok := c.commands[k]; ok {
			return ErrCommandDuplicated
		}

		c.commands[k] = newCommandHandler(c)

		if c.develop {
			log.Println("[PIGEON] command:", k)
		}
	}
	return nil
}

func (c *Client) AddEventProcessor(typ interface{}, processor EventProcessor) error {
	c.types.Put(typ)
	k := internal.ParseType(typ)
	if err := c.broker.Subscribe(k, processor); err != nil {
		return err
	}
	return nil
}

func (c *Client) Dispatch(ctx context.Context, command Command) error {
	k := internal.ParseType(command).String()
	if c.develop {
		log.Println("[PIGEON] dispatch command:", k)
	}
	handler, ok := c.commands[k]
	if !ok {
		return ErrNoSuchCommand
	}
	return handler.Handle(ctx, command)
}
