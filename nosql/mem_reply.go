package nosql

func (r *MemoryStore) Int(reply interface{}, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	v, ok := reply.(int)
	if !ok {
		return 0, ErrWrongType
	}
	return v, nil
}

func (r *MemoryStore) Int64(reply interface{}, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	v, ok := reply.(int64)
	if !ok {
		return 0, ErrWrongType
	}
	return v, nil
}

func (r *MemoryStore) Uint64(reply interface{}, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	v, ok := reply.(uint64)
	if !ok {
		return 0, ErrWrongType
	}
	return v, nil
}

func (r *MemoryStore) Float64(reply interface{}, err error) (float64, error) {
	if err != nil {
		return 0, err
	}
	v, ok := reply.(float64)
	if !ok {
		return 0, ErrWrongType
	}
	return v, nil
}

func (r *MemoryStore) Bool(reply interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	v, ok := reply.(bool)
	if !ok {
		return false, ErrWrongType
	}
	return v, nil
}

func (r *MemoryStore) Bytes(reply interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	v, ok := reply.([]byte)
	if !ok {
		return nil, ErrWrongType
	}
	return v, nil
}

func (r *MemoryStore) String(reply interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	v, ok := reply.(string)
	if !ok {
		return "", ErrWrongType
	}
	return v, nil
}

func (r *MemoryStore) Strings(reply interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	v, ok := reply.([]string)
	if !ok {
		return nil, ErrWrongType
	}
	return v, nil
}

func (r *MemoryStore) Values(reply interface{}, err error) ([]interface{}, error) {
	if err != nil {
		return nil, err
	}
	v, ok := reply.([]interface{})
	if !ok {
		return nil, ErrWrongType
	}
	return v, nil
}
