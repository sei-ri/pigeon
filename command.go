package pigeon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"reflect"

	"github.com/sei-ri/pigeon/internal"
)

type Command interface{}

type CommandHandler interface {
	Handle(context.Context, Command) error
}

type CommandDispatcher interface {
	Dispatch(context.Context, Command) error
}

type commandHandler struct {
	aggregate    reflect.Type
	types        Types
	storage      Storage
	broker       Broker
	debugEnabled bool
}

func newCommandHandler(client *Client) CommandHandler {
	return &commandHandler{
		aggregate:    reflect.TypeOf(client.aggregate).Elem(),
		types:        client.types,
		storage:      client.storage,
		broker:       client.broker,
		debugEnabled: client.develop,
	}
}

func (h *commandHandler) Handle(ctx context.Context, command Command) error {
	aggregate := reflect.New(h.aggregate).Interface().(Aggregate)

	if err := aggregate.Handle(ctx, command); err != nil {
		return err
	}

	aggregateID := aggregate.AggregateID()

	if h.debugEnabled {
		log.Println("[PIGEON] aggregateID:", aggregateID)
	}

	var version int

	// Gets last version by aggregateID
	if v, err := h.storage.Get(ctx, Filter{
		ID: &aggregateID,
	}); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	} else if v != nil && v.Version > 0 {
		version = v.Version
	}

	data := make([]*Data, len(aggregate.Uncommitted()))
	for i, event := range aggregate.Uncommitted() {
		version++
		event.version = version

		if v, err := json.Marshal(event.Data); err != nil {
			return err
		} else {
			data[i] = &Data{
				ID:      aggregateID,
				Version: event.version,
				Type:    internal.ParseType(event.Data).String(),
				Dump:    v,
			}
		}

		if err := h.broker.Publish(event); err != nil {
			return err
		}
	}

	if err := h.storage.Put(ctx, data...); err != nil {
		return err
	}

	return nil
}
