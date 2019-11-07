package dlock

import (
	"sync"
	"testing"
	"time"

	"github.com/ilgooz/mattermost-dlock/dlocktest"
	"github.com/stretchr/testify/require"
)

// TODO(ilgooz): test all branches including related ones to Store errors and ExpireInSeconds.
// TODO(ilgooz): can move tests from sync/mutex_test.go.

func TestLock(t *testing.T) {
	dl := New("a", dlocktest.NewStore())
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				dl.Lock()
				time.Sleep(100 * time.Microsecond)
				dl.Unlock()
			}
		}()
	}
	wg.Wait()
}

func TestLockObtainImmediately(t *testing.T) {
	dl := New("a", dlocktest.NewStore())
	dl.Lock()
	err := dl.Lock(ObtainImmediatelyOption())
	require.Equal(t, ErrCouldntObtainImmediately, err)
}

func TestLockDifferentKeys(t *testing.T) {
	dla := New("a", dlocktest.NewStore())
	dlb := New("b", dlocktest.NewStore())
	dla.Lock()
	dlb.Lock()
	dla.Unlock()
	dlb.Unlock()
}
