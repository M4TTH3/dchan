package dchan

import (
	"bytes"
	"encoding/gob"
)

// gobEncode takes the pointer/interface of value
// and encodes it. e.g. encode(&value).
//
// ensure to register the underlying type e.g. if
// value is a struct, to register it
//
// TODO: maybe caller could pass interface
func gobEncode(value any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// Important to encode the pointer to the value to allow for any type.
	// Users must register the type with gob.Register to allow for decoding.
	if err := enc.Encode(&value); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// gobDecode is the inverse of gobEncode here
func gobDecode(value []byte) (any, error) {
	var v any
	if err := gob.NewDecoder(bytes.NewReader(value)).Decode(&v); err != nil {
		return nil, err
	}

	return v, nil
}
