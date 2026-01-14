package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

type DataType uint8

const (
	TypeInt     DataType = 1
	TypeVarchar DataType = 2
	TypeBool    DataType = 3
	TypeFloat   DataType = 4
	TypeDate    DataType = 5
)

type Column struct {
	Name      string
	Type      DataType
	MaxLen    uint16
	NotNull   bool
	IsPrimary bool
	IsUnique  bool
}

type Tuple struct {
	columns []Column
	values  []interface{}
}

func NewTuple(columns []Column) *Tuple {
	return &Tuple{
		columns: columns,
		values:  make([]interface{}, len(columns)),
	}
}

func (t *Tuple) SetValue(colIdx int, value interface{}) error {
	if colIdx >= len(t.columns) {
		return errors.New("column index out of bounds")
	}

	col := t.columns[colIdx]

	if value == nil && col.NotNull {
		return fmt.Errorf("column %s cannot be null", col.Name)
	}

	if value == nil {
		t.values[colIdx] = nil
		return nil
	}

	switch col.Type {
	case TypeInt:
		if _, ok := value.(int64); !ok {
			return fmt.Errorf("expected int64 for column %s", col.Name)
		}
	case TypeVarchar:
		str, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string for column %s", col.Name)
		}
		if uint16(len(str)) > col.MaxLen {
			return fmt.Errorf("string exceeds max length for column %s", col.Name)
		}
	case TypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool for column %s", col.Name)
		}
	case TypeFloat:
		if _, ok := value.(float64); !ok {
			return fmt.Errorf("expected float64 for column %s", col.Name)
		}
	case TypeDate:
		if _, ok := value.(time.Time); !ok {
			return fmt.Errorf("expected time.Time for column %s", col.Name)
		}
	}

	t.values[colIdx] = value
	return nil
}

func (t *Tuple) GetValue(colIdx int) (interface{}, error) {
	if colIdx >= len(t.columns) {
		return nil, errors.New("column index out of bounds")
	}
	return t.values[colIdx], nil
}

func (t *Tuple) Serialize() []byte {
	buf := make([]byte, 0)

	for i, col := range t.columns {
		val := t.values[i]

		if val == nil {
			buf = append(buf, 0)
			continue
		}

		buf = append(buf, 1)

		switch col.Type {
		case TypeInt:
			v := val.(int64)
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(v))
			buf = append(buf, b...)

		case TypeVarchar:
			v := val.(string)
			lenBytes := make([]byte, 2)
			binary.LittleEndian.PutUint16(lenBytes, uint16(len(v)))
			buf = append(buf, lenBytes...)
			buf = append(buf, []byte(v)...)

		case TypeBool:
			v := val.(bool)
			if v {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}

		case TypeFloat:
			v := val.(float64)
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, math.Float64bits(v))
			buf = append(buf, b...)

		case TypeDate:
			v := val.(time.Time)
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(v.Unix()))
			buf = append(buf, b...)
		}
	}

	return buf
}

func (t *Tuple) Deserialize(data []byte) error {
	offset := 0

	for i, col := range t.columns {
		if offset >= len(data) {
			return errors.New("insufficient data")
		}

		isNull := data[offset] == 0
		offset++

		if isNull {
			t.values[i] = nil
			continue
		}

		switch col.Type {
		case TypeInt:
			if offset+8 > len(data) {
				return errors.New("insufficient data for int")
			}
			val := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			t.values[i] = val
			offset += 8

		case TypeVarchar:
			if offset+2 > len(data) {
				return errors.New("insufficient data for varchar length")
			}
			length := binary.LittleEndian.Uint16(data[offset : offset+2])
			offset += 2
			if offset+int(length) > len(data) {
				return errors.New("insufficient data for varchar content")
			}
			val := string(data[offset : offset+int(length)])
			t.values[i] = val
			offset += int(length)

		case TypeBool:
			if offset+1 > len(data) {
				return errors.New("insufficient data for bool")
			}
			val := data[offset] == 1
			t.values[i] = val
			offset += 1

		case TypeFloat:
			if offset+8 > len(data) {
				return errors.New("insufficient data for float")
			}
			bits := binary.LittleEndian.Uint64(data[offset : offset+8])
			val := math.Float64frombits(bits)
			t.values[i] = val
			offset += 8

		case TypeDate:
			if offset+8 > len(data) {
				return errors.New("insufficient data for date")
			}
			timestamp := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			val := time.Unix(timestamp, 0)
			t.values[i] = val
			offset += 8
		}
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
