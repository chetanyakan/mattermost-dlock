// Package dlock is a distributed lock to enable advanced synchronization for Mattermost Plugins.
//
// if you're new to distributed locks and Mattermost Plugins please read sample use case scenarios
// at: https://community.mattermost.com/core/pl/bb376sjsdbym8kj7nz7zrcos7r
package dlock

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mattermost/mattermost-server/model"
)

const (
	// storePrefix used to prefix lock related keys in KV store.
	storePrefix = "dlock:"
)

const (
	// lockTTL is lock's expiry time.
	lockTTL = time.Second * 15

	// lockRefreshInterval used to determine how long to wait before refreshing
	// a lock's expiry time.
	lockRefreshInterval = time.Second

	// lockTryInterval used to wait before trying to obtain the lock again.
	lockTryInterval = time.Second
)

var (
	// ErrCouldntObtainImmediately returned when a lock couldn't be obtained immediately after
	// calling Lock().
	ErrCouldntObtainImmediately = errors.New("could not obtain immediately")
)

// Store is a data store to keep locks' state.
type Store interface {
	KVSetWithOptions(key string, newValue interface{}, options model.PluginKVSetOptions) (bool, *model.AppError)
	KVDelete(key string) *model.AppError
}

// DLock is a distributed lock.
type DLock struct {
	// store used to store lock's state to do synchronization.
	store Store

	// key to lock for.
	key string

	// defaultOptions are overwritten by call to Lock() or RLock().
	defaultOptions []Option

	// refreshCancel stops refreshing lock's TTL.
	refreshCancel context.CancelFunc

	// refreshWait is a waiter to make sure refreshing is finished.
	refreshWait *sync.WaitGroup
}

// configuration keeps lock configurations.
type configuration struct {
	ctx               context.Context
	obtainImmediately bool
}

// Option modifies configuration.
type Option func(*configuration)

// ContextOption provides a context to locking. when ctx is cancelled,
// Lock() will stop blocking and return with error.
func ContextOption(ctx context.Context) Option {
	return func(c *configuration) {
		c.ctx = ctx
	}
}

// ObtainImmediatelyOption tries to Lock() immediately.
// if cannot, Lock() will return with an error.
func ObtainImmediatelyOption() Option {
	return func(c *configuration) {
		c.obtainImmediately = true
	}
}

// New creates a new distributed lock for key on given store with options.
// think,
//   `dl := New("my-key", store)`
// as an equivalent of,
//   `var m sync.Mutex`
// and use it in the same way.
func New(key string, store Store, options ...Option) *DLock {
	d := &DLock{
		key:            buildKey(key),
		defaultOptions: options,
		store:          store,
	}
	return d
}

// createConfig creates a new config by merging options with default ones.
func (d *DLock) createConfig(options ...Option) *configuration {
	c := &configuration{}
	options = append(d.defaultOptions, options...)
	for _, o := range options {
		o(c)
	}
	if c.ctx == nil {
		c.ctx = context.Background()
	}
	return c
}

// Lock obtains a new lock.
// use Lock() exactly like sync.Mutex.Lock(), avoid missuses like deadlocks.
func (d *DLock) Lock(options ...Option) error {
	kopts := model.PluginKVSetOptions{
		EncodeJSON:      true,
		Atomic:          true,
		OldValue:        nil,
		ExpireInSeconds: int64(lockTTL.Seconds()),
	}
	conf := d.createConfig(options...)
	for {
		_, aerr := d.store.KVSetWithOptions(d.key, true, kopts)
		isLockObtained := aerr == nil
		if isLockObtained {
			d.startRefreshLoop()
			return nil
		}
		if conf.obtainImmediately {
			return ErrCouldntObtainImmediately
		}
		afterC := time.After(lockTryInterval)
		select {
		case <-conf.ctx.Done():
			return conf.ctx.Err()
		case <-afterC:
		}
	}
}

// startRefreshLoop refreshes an obtained lock to not get caught by lock's TTL.
// TTL tends to hit and release the lock automatically when plugin terminates.
func (d *DLock) startRefreshLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(lockRefreshInterval)
		kopts := model.PluginKVSetOptions{
			EncodeJSON:      true,
			ExpireInSeconds: int64(lockTTL.Seconds()),
		}
		for {
			select {
			case <-t.C:
				d.store.KVSetWithOptions(d.key, true, kopts)
			case <-ctx.Done():
				return
			}
		}
	}()
	d.refreshCancel = cancel
	d.refreshWait = &wg
}

// Unlock unlocks Lock().
// use Unlock() exactly like sync.Mutex.Unlock().
func (d *DLock) Unlock() error {
	d.refreshCancel()
	d.refreshWait.Wait()
	aerr := d.store.KVDelete(d.key)
	return normalizeAppErr(aerr)
}

// buildKey builds a lock key for KV store.
func buildKey(key string) string {
	return storePrefix + key
}

// normalize error normalizes Plugin API's errors.
// please see this docs to know more about what this normalization do: https://golang.org/doc/faq#nil_error
func normalizeAppErr(err *model.AppError) error {
	if err == nil {
		return nil
	}
	return err
}
