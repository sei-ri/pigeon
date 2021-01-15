package pigeon

import (
	"context"
	"fmt"
)

var _ Aggregate = &AggregateMixin{}

type Aggregate interface {
	AggregateID() string
	Uncommitted() []Event
	Committed()
	Replay([]Event) error
	Apply(agg Aggregate, event Event, isNew bool) error
	ApplyChange(Event) error
	CommandHandler
}

type AggregateMixin struct {
	ID      string
	version int
	changes []Event
}

func (a *AggregateMixin) AggregateID() string {
	return a.ID
}

func (a *AggregateMixin) Uncommitted() []Event {
	return a.changes
}

func (a *AggregateMixin) Committed() {
	a.changes = make([]Event, 0)
}

func (a *AggregateMixin) Replay(history []Event) error {
	for i := range history {
		if err := a.Apply(a, history[i], false); err != nil {
			return err
		}
	}
	return nil
}

func (a *AggregateMixin) Apply(agg Aggregate, event Event, isNew bool) error {
	a.version++

	if err := agg.ApplyChange(event); err != nil {
		return err
	}

	if isNew {
		event.version = a.version
		a.changes = append(a.changes, event)
	}

	return nil
}

func (a *AggregateMixin) ApplyChange(_ Event) error {
	return fmt.Errorf("[PIGEON] not implemented ApplyChange")
}

func (a *AggregateMixin) Handle(_ context.Context, _ Command) error {
	return fmt.Errorf("[PIGEON] not implemented Handle")
}
