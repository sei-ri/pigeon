package pigeon

import (
	"reflect"
	"sync"

	"github.com/sei-ri/pigeon/internal"
)

type Event struct {
	version int
	Data    interface{}
}

// EventProcessor is event callback (subscribe)
type EventProcessor func(Event) error

// Types is event type memroy store
type Types interface {
	Put(i interface{})
	Get(s string) reflect.Type
}

// Types is event type memroy store
type types struct {
	mu    *sync.Mutex
	items map[string]reflect.Type
}

func newTypes() Types {
	return &types{
		mu:    &sync.Mutex{},
		items: map[string]reflect.Type{}}
}

func (a *types) Put(i interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()

	t := internal.ParseType(i)
	a.items[t.String()] = t
}

func (a *types) Get(s string) reflect.Type {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.items[s]
}
