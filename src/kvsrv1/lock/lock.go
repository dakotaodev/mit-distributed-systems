package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	id string
	lockName string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck: ck,
		id: kvtest.RandValue(8),
		lockName: lockname,
	}
	return lk
}

func (lk *Lock) Acquire() {

	for {
		value, version, err := lk.ck.Get(lk.lockName)
		switch err {
		case rpc.ErrNoKey:
			// this lock has not been attempted, let's create it
			if ok := lk.ck.Put(lk.lockName, lk.id, 0); ok == rpc.OK {
				return 
			} else if ok == rpc.ErrMaybe {
				if value, _, _ := lk.ck.Get(lk.lockName); value == lk.id {
					return 
				}
			}
			// if the lock was not acquired, we will retry
		case rpc.OK:
			// the key exists, we need to validate that the lock is not held
			if value == "" {
				// perform put to claim the lock
				if ok := lk.ck.Put(lk.lockName, lk.id, version); ok==rpc.OK {
					return
				} else if ok==rpc.ErrMaybe {
					
					if value, _, _ := lk.ck.Get(lk.lockName); value == lk.id {
						return
					}
				}
			}
		}
		// retry loop after time out
		time.Sleep(time.Millisecond * 100)
	}
		
}

func (lk *Lock) Release() {
	// Your code here
	for {
		_, version, err := lk.ck.Get(lk.lockName)
		if err == rpc.OK {
			if ok := lk.ck.Put(lk.lockName, "", version); ok == rpc.OK {
				return
			} else if ok ==rpc.ErrMaybe {
				
				if value, _, _ := lk.ck.Get(lk.lockName); value == lk.id {
					return
				}
			}
		}
		time.Sleep(time.Second)
	}
}
