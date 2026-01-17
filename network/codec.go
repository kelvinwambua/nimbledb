package network

import (
    "bytes"
    "encoding/binary"
    "errors"
    "fmt"
    "time"
)

type Codec struct{}

func NewCodec() *Codec {
    return &Codec{}
}

func (c *Codec) EncodeString(s string) []byte {
    buf := new(bytes.Buffer)
    length := uint32(len(s))
    binary.Write(buf, binary.BigEndian, length)
    buf.WriteString(s)
    return buf.Bytes()
}

func (c *Codec) DecodeString(data []byte) (string, int, error) {
    if len(data) < 4 {
        return "", 0, errors.New("insufficient data for string length")
    }

    length := binary.BigEndian.Uint32(data[0:4])
    if len(data) < int(4+length) {
        return "", 0, errors.New("insufficient data for string content")
    }

    str := string(data[4 : 4+length])
    return str, int(4 + length), nil
}

func (c *Codec) EncodeInt64(n int64) []byte {
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, uint64(n))
    return buf
}

func (c *Codec) DecodeInt64(data []byte) (int64, int, error) {
    if len(data) < 8 {
        return 0, 0, errors.New("insufficient data for int64")
    }
    n := int64(binary.BigEndian.Uint64(data[0:8]))
    return n, 8, nil
}

func (c *Codec) EncodeFloat64(f float64) []byte {
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, uint64(f))
    return buf
}

func (c *Codec) DecodeFloat64(data []byte) (float64, int, error) {
    if len(data) < 8 {
        return 0, 0, errors.New("insufficient data for float64")
    }
    f := float64(binary.BigEndian.Uint64(data[0:8]))
    return f, 8, nil
}

func (c *Codec) EncodeBool(b bool) []byte {
    if b {
        return []byte{1}
    }
    return []byte{0}
}

func (c *Codec) DecodeBool(data []byte) (bool, int, error) {
    if len(data) < 1 {
        return false, 0, errors.New("insufficient data for bool")
    }
    return data[0] == 1, 1, nil
}

func (c *Codec) EncodeTime(t time.Time) []byte {
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, uint64(t.Unix()))
    return buf
}

func (c *Codec) DecodeTime(data []byte) (time.Time, int, error) {
    if len(data) < 8 {
        return time.Time{}, 0, errors.New("insufficient data for time")
    }
    timestamp := int64(binary.BigEndian.Uint64(data[0:8]))
    return time.Unix(timestamp, 0), 8, nil
}

func (c *Codec) EncodeValue(v interface{}) ([]byte, error) {
    buf := new(bytes.Buffer)

    switch val := v.(type) {
    case nil:
        buf.WriteByte(0)
    case int64:
        buf.WriteByte(1)
        buf.Write(c.EncodeInt64(val))
    case string:
        buf.WriteByte(2)
        buf.Write(c.EncodeString(val))
    case bool:
        buf.WriteByte(3)
        buf.Write(c.EncodeBool(val))
    case float64:
        buf.WriteByte(4)
        buf.Write(c.EncodeFloat64(val))
    case time.Time:
        buf.WriteByte(5)
        buf.Write(c.EncodeTime(val))
    default:
        return nil, fmt.Errorf("unsupported type: %T", v)
    }

    return buf.Bytes(), nil
}

func (c *Codec) DecodeValue(data []byte) (interface{}, int, error) {
    if len(data) < 1 {
        return nil, 0, errors.New("insufficient data for type tag")
    }

    typeTag := data[0]
    offset := 1

    switch typeTag {
    case 0:
        return nil, 1, nil
    case 1:
        val, n, err := c.DecodeInt64(data[offset:])
        return val, offset + n, err
    case 2:
        val, n, err := c.DecodeString(data[offset:])
        return val, offset + n, err
    case 3:
        val, n, err := c.DecodeBool(data[offset:])
        return val, offset + n, err
    case 4:
        val, n, err := c.DecodeFloat64(data[offset:])
        return val, offset + n, err
    case 5:
        val, n, err := c.DecodeTime(data[offset:])
        return val, offset + n, err
    default:
        return nil, 0, fmt.Errorf("unknown type tag: %d", typeTag)
    }
}

func (c *Codec) EncodeRow(row []interface{}) ([]byte, error) {
    buf := new(bytes.Buffer)

    binary.Write(buf, binary.BigEndian, uint32(len(row)))

    for _, val := range row {
        encoded, err := c.EncodeValue(val)
        if err != nil {
            return nil, err
        }
        buf.Write(encoded)
    }

    return buf.Bytes(), nil
}

func (c *Codec) DecodeRow(data []byte) ([]interface{}, int, error) {
    if len(data) < 4 {
        return nil, 0, errors.New("insufficient data for row count")
    }

    count := binary.BigEndian.Uint32(data[0:4])
    offset := 4

    row := make([]interface{}, count)
    for i := uint32(0); i < count; i++ {
        val, n, err := c.DecodeValue(data[offset:])
        if err != nil {
            return nil, 0, err
        }
        row[i] = val
        offset += n
    }

    return row, offset, nil
}

func (c *Codec) EncodeResultSet(columns []string, rows [][]interface{}) ([]byte, error) {
    buf := new(bytes.Buffer)

    binary.Write(buf, binary.BigEndian, uint32(len(columns)))
    for _, col := range columns {
        buf.Write(c.EncodeString(col))
    }

    binary.Write(buf, binary.BigEndian, uint32(len(rows)))
    for _, row := range rows {
        encoded, err := c.EncodeRow(row)
        if err != nil {
            return nil, err
        }
        buf.Write(encoded)
    }

    return buf.Bytes(), nil
}

func (c *Codec) DecodeResultSet(data []byte) ([]string, [][]interface{}, error) {
    if len(data) < 4 {
        return nil, nil, errors.New("insufficient data for column count")
    }

    colCount := binary.BigEndian.Uint32(data[0:4])
    offset := 4

    columns := make([]string, colCount)
    for i := uint32(0); i < colCount; i++ {
        col, n, err := c.DecodeString(data[offset:])
        if err != nil {
            return nil, nil, err
        }
        columns[i] = col
        offset += n
    }

    if len(data) < offset+4 {
        return nil, nil, errors.New("insufficient data for row count")
    }

    rowCount := binary.BigEndian.Uint32(data[offset : offset+4])
    offset += 4

    rows := make([][]interface{}, rowCount)
    for i := uint32(0); i < rowCount; i++ {
        row, n, err := c.DecodeRow(data[offset:])
        if err != nil {
            return nil, nil, err
        }
        rows[i] = row
        offset += n
    }

    return columns, rows, nil
}
