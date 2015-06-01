package nosql

import (
	"sync"
	"time"
)

type MemoryStore struct {
	cache map[string]valitem
	l     sync.Mutex
}

type valitem struct {
	Val    interface{}
	Expire int64
}

func NewMemoryStore() (*MemoryStore, error) {
	s := new(MemoryStore)
	s.cache = make(map[string]valitem)
	return s, nil
}

func (m *MemoryStore) SetMaxIdle(int) {
	//do nothing
}
func (m *MemoryStore) SetMaxActive(int) {
	//do nothing
}

func (m *MemoryStore) Get(key string) (interface{}, error) {
	m.l.Lock()
	v, ok := m.get(key)
	m.l.Unlock()
	if !ok {
		return nil, ErrNil
	}
	return v.Val, nil
}

func (m *MemoryStore) Set(key string, value interface{}) error {
	m.l.Lock()
	m.set(key, value, 0)
	m.l.Unlock()
	return nil
}

func (m *MemoryStore) SetEx(key string, value interface{}, timeout int64) error {
	m.l.Lock()
	m.set(key, value, time.Now().Unix()+timeout)
	m.l.Unlock()
	return nil
}
func (m *MemoryStore) SetNx(key string, value interface{}) (int64, error) {
	m.l.Lock()
	if _, ok := m.get(key); ok {
		m.l.Unlock()
		return 0, nil
	}
	m.set(key, value, 0)
	m.l.Unlock()
	return 1, nil
}

func (m *MemoryStore) ExpireAt(key string, timestamp int64) (int64, error) {
	m.l.Lock()
	if v, ok := m.get(key); ok {
		v.Expire = timestamp
		m.l.Unlock()
		return 1, nil
	}
	m.l.Unlock()
	return 0, nil
}

func (m *MemoryStore) Del(keys ...string) (int64, error) {
	var count int64 = 0
	m.l.Lock()
	for _, k := range keys {
		if _, ok := m.get(k); ok {
			count++
			delete(m.cache, k)
		}
	}
	m.l.Unlock()
	return count, nil
}

func (m *MemoryStore) Incr(key string) (int64, error) {
	return m.IncrBy(key, 1)
}

func (m *MemoryStore) IncrBy(key string, delta int64) (int64, error) {
	m.l.Lock()
	v, ok := m.get(key)
	if !ok {
		m.set(key, delta, 0)
		m.l.Unlock()
		return delta, nil
	}
	n, err := m.Int64(v.Val, nil)
	if err != nil {
		m.l.Unlock()
		return 0, err
	}
	n += delta
	m.set(key, n, 0)
	m.l.Unlock()
	return n, nil
}

func (m *MemoryStore) Expire(key string, duration int64) (int64, error) {
	//TODO
	panic("unimplemented")
	return 0, nil
}

func (m *MemoryStore) HGet(key string, field string) (interface{}, error) {
	m.l.Lock()
	v, ok := m.get(key + field)
	m.l.Unlock()
	if !ok {
		return nil, ErrNil
	}
	return v.Val, nil
}

func (m *MemoryStore) HLen(key string) (int64, error) {
	//TODO
	return 0, nil
}

func (m *MemoryStore) HSet(key string, field string, val interface{}) error {
	m.l.Lock()
	m.set(key+field, val, 0)
	m.l.Unlock()
	return nil
}
func (m *MemoryStore) HDel(key string, fields ...string) (int64, error) {
	m.l.Lock()
	for _, f := range fields {
		delete(m.cache, key+f)
	}
	m.l.Unlock()
	return 0, nil
}

func (m *MemoryStore) HClear(key string) error {
	//TODO
	return nil
}

func (m *MemoryStore) HMGet(string, ...string) (interface{}, error) {
	//TODO
	panic("unimplemented")
	return nil, nil
}

func (m *MemoryStore) HMSet(string, ...interface{}) error {
	//TODO
	panic("unimplemented")
	return nil
}

func (m *MemoryStore) HExpire(string, int64) error {
	//TODO
	//panic("unimplemented")
	//do nothing
	return nil
}

func (m *MemoryStore) HGetAll(string) (map[string]interface{}, error) {
	//TODO
	panic("unimplemented")
	return nil, nil
}

func (m *MemoryStore) HIncrBy(string, string, int64) (int64, error) {
	//TODO
	panic("unimplemented")
	return 0, nil
}

func (m *MemoryStore) ZAdd(string, ...interface{}) (int64, error) {
	//TODO
	panic("unimplemented")
	return 0, nil
}

func (m *MemoryStore) ZRem(string, ...string) (int64, error) {
	//TODO
	panic("unimplemented")
	return 0, nil
}

func (m *MemoryStore) ZExpire(string, int64) error {
	//TODO
	panic("unimplemented")
	return nil
}
func (m *MemoryStore) ZRange(string, int64, int64, bool) (interface{}, error) {
	//TODO
	panic("unimplemented")
	return nil, nil
}

func (m *MemoryStore) ZRangeByScoreWithScore(key string, min, max int64) (map[string]int64, error) {
	panic("unimplemented")
	return nil, nil
}

func (m *MemoryStore) ZClear(key string) error {
	// TODO timeout
	panic("unimplemented")
	return nil
}

func (m *MemoryStore) LRange(key string, start, stop int64) (interface{}, error) {
	panic("unimplemented")
	return nil, nil
}

func (m *MemoryStore) LLen(key string) (int64, error) {
	panic("unimplemented")
	return 0, nil
}

func (m *MemoryStore) LPop(key string) (interface{}, error) {
	panic("unimplemented")
	return nil, nil
}

func (m *MemoryStore) RPush(key string, value ...interface{}) error {
	panic("unimplemented")
	return nil
}

func (m *MemoryStore) SAdd(key string, members ...interface{}) (int64, error) {
	panic("unimplemented")
	return 0, nil
}

func (m *MemoryStore) SIsMember(key string, member interface{}) (bool, error) {
	panic("unimplemented")
	return false, nil
}

func (m *MemoryStore) SRem(key string, members ...interface{}) (int64, error) {
	panic("unimplemented")
	return 0, nil
}

///////////////////////////////////////////////////////////////

func (m *MemoryStore) get(key string) (valitem, bool) {
	v, ok := m.cache[key]
	if !ok {
		return nilItem, false
	}
	if v.Expire > 0 {
		if v.Expire < time.Now().Unix() {
			delete(m.cache, key)
			return nilItem, false
		}
	}
	return v, true
}

func (m *MemoryStore) set(key string, val interface{}, expire int64) {
	m.cache[key] = valitem{val, expire}
}

var nilItem = valitem{nil, 0}
