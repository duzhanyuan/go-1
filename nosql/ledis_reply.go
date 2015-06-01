package nosql

import (
	"github.com/kingsoft-wps/go/nosql/ledis"
)

func (r *LedisStore) Int(reply interface{}, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	res, err := ledis.Int(reply, err)
	if err == ledis.ErrNil {
		return 0, ErrNil
	}
	return res, err
}

func (r *LedisStore) Int64(reply interface{}, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	res, err := ledis.Int64(reply, err)
	if err == ledis.ErrNil {
		return 0, ErrNil
	}
	return res, err
}

func (r *LedisStore) Uint64(reply interface{}, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	res, err := ledis.Uint64(reply, err)
	if err == ledis.ErrNil {
		return 0, ErrNil
	}
	return res, err
}

func (r *LedisStore) Float64(reply interface{}, err error) (float64, error) {
	if err != nil {
		return 0, err
	}
	res, err := ledis.Float64(reply, err)
	if err == ledis.ErrNil {
		return 0, ErrNil
	}
	return res, err
}

func (r *LedisStore) Bool(reply interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	res, err := ledis.Bool(reply, err)
	if err == ledis.ErrNil {
		return false, ErrNil
	}
	return res, err
}

func (r *LedisStore) Bytes(reply interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	res, err := ledis.Bytes(reply, err)
	if err == ledis.ErrNil {
		return nil, ErrNil
	}
	return res, err
}

func (r *LedisStore) String(reply interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	res, err := ledis.String(reply, err)
	if err == ledis.ErrNil {
		return "", ErrNil
	}
	return res, err
}

func (r *LedisStore) Strings(reply interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	res, err := ledis.Strings(reply, err)
	if err == ledis.ErrNil {
		return nil, ErrNil
	}
	return res, err
}

func (r *LedisStore) Values(reply interface{}, err error) ([]interface{}, error) {
	if err != nil {
		return nil, err
	}
	res, err := ledis.Values(reply, err)
	if err == ledis.ErrNil {
		return nil, ErrNil
	}
	return res, err
}
