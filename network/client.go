package network

import (
    "fmt"
    "net"
    "sync"
    "sync/atomic"
)

type Client struct {
    addr   string
    conn   net.Conn
    codec  *Codec
    seqID  atomic.Uint32
    mu     sync.Mutex
}

func NewClient(addr string) *Client {
    return &Client{
        addr:  addr,
        codec: NewCodec(),
    }
}

func (c *Client) Connect() error {
    conn, err := net.Dial("tcp", c.addr)
    if err != nil {
        return fmt.Errorf("failed to connect: %w", err)
    }

    c.conn = conn

    handshake := []byte("RDBMS Client")
    if err := WriteMessage(c.conn, MsgHandshake, handshake, 0); err != nil {
        c.conn.Close()
        return err
    }

    msg, _, err := ReadMessage(c.conn)
    if err != nil {
        c.conn.Close()
        return err
    }

    if msg.Type != MsgHandshake {
        c.conn.Close()
        return fmt.Errorf("unexpected handshake response")
    }

    return nil
}

func (c *Client) Close() error {
    if c.conn != nil {
        return c.conn.Close()
    }
    return nil
}

func (c *Client) Query(query string) ([]string, [][]interface{}, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    payload := c.codec.EncodeString(query)
    seqID := c.seqID.Add(1)

    if err := WriteMessage(c.conn, MsgQuery, payload, seqID); err != nil {
        return nil, nil, err
    }

    msg, _, err := ReadMessage(c.conn)
    if err != nil {
        return nil, nil, err
    }

    if msg.Type == MsgError {
        errMsg, _, _ := c.codec.DecodeString(msg.Payload)
        return nil, nil, fmt.Errorf("server error: %s", errMsg)
    }

    if msg.Type != MsgQueryResult {
        return nil, nil, fmt.Errorf("unexpected response type: %d", msg.Type)
    }

    return c.codec.DecodeResultSet(msg.Payload)
}

func (c *Client) Prepare(query string) (uint32, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    payload := c.codec.EncodeString(query)
    seqID := c.seqID.Add(1)

    if err := WriteMessage(c.conn, MsgPreparedStmt, payload, seqID); err != nil {
        return 0, err
    }

    msg, _, err := ReadMessage(c.conn)
    if err != nil {
        return 0, err
    }

    if msg.Type == MsgError {
        errMsg, _, _ := c.codec.DecodeString(msg.Payload)
        return 0, fmt.Errorf("server error: %s", errMsg)
    }

    stmtID, _, err := c.codec.DecodeInt64(msg.Payload)
    return uint32(stmtID), err
}

func (c *Client) Execute(stmtID uint32, params ...interface{}) ([]string, [][]interface{}, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    payload := c.codec.EncodeInt64(int64(stmtID))
    for _, param := range params {
        encoded, err := c.codec.EncodeValue(param)
        if err != nil {
            return nil, nil, err
        }
        payload = append(payload, encoded...)
    }

    seqID := c.seqID.Add(1)

    if err := WriteMessage(c.conn, MsgExecuteStmt, payload, seqID); err != nil {
        return nil, nil, err
    }

    msg, _, err := ReadMessage(c.conn)
    if err != nil {
        return nil, nil, err
    }

    if msg.Type == MsgError {
        errMsg, _, _ := c.codec.DecodeString(msg.Payload)
        return nil, nil, fmt.Errorf("server error: %s", errMsg)
    }

    return c.codec.DecodeResultSet(msg.Payload)
}

func (c *Client) Ping() error {
    c.mu.Lock()
    defer c.mu.Unlock()

    seqID := c.seqID.Add(1)

    if err := WriteMessage(c.conn, MsgPing, []byte{}, seqID); err != nil {
        return err
    }

    msg, _, err := ReadMessage(c.conn)
    if err != nil {
        return err
    }

    if msg.Type != MsgPong {
        return fmt.Errorf("unexpected response to ping")
    }

    return nil
}
