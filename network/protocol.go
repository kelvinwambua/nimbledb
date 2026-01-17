
package network

import (
    "encoding/binary"
    "errors"
    "io"
)

type MessageType uint8

const (
    MsgHandshake MessageType = iota
    MsgQuery
    MsgQueryResult
    MsgPreparedStmt
    MsgExecuteStmt
    MsgCloseStmt
    MsgBeginTx
    MsgCommitTx
    MsgRollbackTx
    MsgError
    MsgPing
    MsgPong
    MsgAuth
    MsgAuthOk
)

const (
    ProtocolVersion = 1
    MaxMessageSize  = 16 * 1024 * 1024
)

type Message struct {
    Type    MessageType
    Payload []byte
}

type MessageHeader struct {
    Version     uint8
    MessageType MessageType
    Length      uint32
    SequenceID  uint32
}

func (h *MessageHeader) Encode() []byte {
    buf := make([]byte, 10)
    buf[0] = h.Version
    buf[1] = byte(h.MessageType)
    binary.BigEndian.PutUint32(buf[2:6], h.Length)
    binary.BigEndian.PutUint32(buf[6:10], h.SequenceID)
    return buf
}

func (h *MessageHeader) Decode(data []byte) error {
    if len(data) < 10 {
        return errors.New("insufficient data for header")
    }
    h.Version = data[0]
    h.MessageType = MessageType(data[1])
    h.Length = binary.BigEndian.Uint32(data[2:6])
    h.SequenceID = binary.BigEndian.Uint32(data[6:10])

    if h.Version != ProtocolVersion {
        return errors.New("unsupported protocol version")
    }
    if h.Length > MaxMessageSize {
        return errors.New("message size exceeds maximum")
    }

    return nil
}

func WriteMessage(w io.Writer, msgType MessageType, payload []byte, seqID uint32) error {
    header := &MessageHeader{
        Version:     ProtocolVersion,
        MessageType: msgType,
        Length:      uint32(len(payload)),
        SequenceID:  seqID,
    }

    headerBytes := header.Encode()
    if _, err := w.Write(headerBytes); err != nil {
        return err
    }

    if len(payload) > 0 {
        if _, err := w.Write(payload); err != nil {
            return err
        }
    }

    return nil
}

func ReadMessage(r io.Reader) (*Message, uint32, error) {
    headerBytes := make([]byte, 10)
    if _, err := io.ReadFull(r, headerBytes); err != nil {
        return nil, 0, err
    }

    header := &MessageHeader{}
    if err := header.Decode(headerBytes); err != nil {
        return nil, 0, err
    }

    payload := make([]byte, header.Length)
    if header.Length > 0 {
        if _, err := io.ReadFull(r, payload); err != nil {
            return nil, 0, err
        }
    }

    return &Message{
        Type:    header.MessageType,
        Payload: payload,
    }, header.SequenceID, nil
}
