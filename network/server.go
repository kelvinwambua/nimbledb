package network

import (
    "context"
    "fmt"
    "log"
    "net"
    "sync"
    "sync/atomic"
)

type Handler interface {
    HandleQuery(query string) ([]string, [][]interface{}, error)
    HandlePrepare(query string) (uint32, error)
    HandleExecute(stmtID uint32, params []interface{}) ([]string, [][]interface{}, error)
    HandleClose(stmtID uint32) error
    HandleBeginTx() (uint64, error)
    HandleCommitTx(txID uint64) error
    HandleRollbackTx(txID uint64) error
}

type Server struct {
    addr      string
    handler   Handler
    listener  net.Listener
    codec     *Codec
    wg        sync.WaitGroup
    ctx       context.Context
    cancel    context.CancelFunc
    connCount atomic.Int32
}

func NewServer(addr string, handler Handler) *Server {
    ctx, cancel := context.WithCancel(context.Background())
    return &Server{
        addr:    addr,
        handler: handler,
        codec:   NewCodec(),
        ctx:     ctx,
        cancel:  cancel,
    }
}

func (s *Server) Start() error {
    listener, err := net.Listen("tcp", s.addr)
    if err != nil {
        return fmt.Errorf("failed to start server: %w", err)
    }

    s.listener = listener
    log.Printf("Server listening on %s", s.addr)

    s.wg.Add(1)
    go s.acceptLoop()

    return nil
}

func (s *Server) Stop() error {
    s.cancel()
    if s.listener != nil {
        s.listener.Close()
    }
    s.wg.Wait()
    log.Println("Server stopped")
    return nil
}

func (s *Server) acceptLoop() {
    defer s.wg.Done()

    for {
        select {
        case <-s.ctx.Done():
            return
        default:
        }

        conn, err := s.listener.Accept()
        if err != nil {
            select {
            case <-s.ctx.Done():
                return
            default:
                log.Printf("Accept error: %v", err)
                continue
            }
        }

        s.wg.Add(1)
        go s.handleConnection(conn)
    }
}

func (s *Server) handleConnection(conn net.Conn) {
    defer s.wg.Done()
    defer conn.Close()

    connID := s.connCount.Add(1)
    log.Printf("Client %d connected from %s", connID, conn.RemoteAddr())

    session := &Session{
        conn:   conn,
        codec:  s.codec,
        seqID:  0,
        connID: connID,
    }

    if err := s.handleHandshake(session); err != nil {
        log.Printf("Client %d handshake failed: %v", connID, err)
        return
    }

    for {
        select {
        case <-s.ctx.Done():
            return
        default:
        }

        msg, seqID, err := ReadMessage(conn)
        if err != nil {
            if err.Error() != "EOF" {
                log.Printf("Client %d read error: %v", connID, err)
            }
            return
        }

        session.seqID = seqID

        if err := s.handleMessage(session, msg); err != nil {
            log.Printf("Client %d message handling error: %v", connID, err)
            s.sendError(session, err)
        }
    }
}

func (s *Server) handleHandshake(session *Session) error {
    msg, _, err := ReadMessage(session.conn)
    if err != nil {
        return err
    }

    if msg.Type != MsgHandshake {
        return fmt.Errorf("expected handshake message, got %d", msg.Type)
    }

    response := []byte(fmt.Sprintf("RDBMS v%d", ProtocolVersion))
    return WriteMessage(session.conn, MsgHandshake, response, 0)
}

func (s *Server) handleMessage(session *Session, msg *Message) error {
    switch msg.Type {
    case MsgQuery:
        return s.handleQuery(session, msg.Payload)
    case MsgPreparedStmt:
        return s.handlePrepare(session, msg.Payload)
    case MsgExecuteStmt:
        return s.handleExecute(session, msg.Payload)
    case MsgCloseStmt:
        return s.handleCloseStmt(session, msg.Payload)
    case MsgBeginTx:
        return s.handleBeginTx(session)
    case MsgCommitTx:
        return s.handleCommitTx(session, msg.Payload)
    case MsgRollbackTx:
        return s.handleRollbackTx(session, msg.Payload)
    case MsgPing:
        return s.handlePing(session)
    default:
        return fmt.Errorf("unsupported message type: %d", msg.Type)
    }
}

func (s *Server) handleQuery(session *Session, payload []byte) error {
    query, _, err := s.codec.DecodeString(payload)
    if err != nil {
        return err
    }

    log.Printf("Client %d executing query: %s", session.connID, query)

    columns, rows, err := s.handler.HandleQuery(query)
    if err != nil {
        return err
    }

    resultData, err := s.codec.EncodeResultSet(columns, rows)
    if err != nil {
        return err
    }

    return WriteMessage(session.conn, MsgQueryResult, resultData, session.seqID+1)
}

func (s *Server) handlePrepare(session *Session, payload []byte) error {
    query, _, err := s.codec.DecodeString(payload)
    if err != nil {
        return err
    }

    stmtID, err := s.handler.HandlePrepare(query)
    if err != nil {
        return err
    }

    response := s.codec.EncodeInt64(int64(stmtID))
    return WriteMessage(session.conn, MsgPreparedStmt, response, session.seqID+1)
}

func (s *Server) handleExecute(session *Session, payload []byte) error {
    stmtID, n, err := s.codec.DecodeInt64(payload)
    if err != nil {
        return err
    }

    params := make([]interface{}, 0)
    offset := n

    for offset < len(payload) {
        param, paramN, err := s.codec.DecodeValue(payload[offset:])
        if err != nil {
            return err
        }
        params = append(params, param)
        offset += paramN
    }

    columns, rows, err := s.handler.HandleExecute(uint32(stmtID), params)
    if err != nil {
        return err
    }

    resultData, err := s.codec.EncodeResultSet(columns, rows)
    if err != nil {
        return err
    }

    return WriteMessage(session.conn, MsgQueryResult, resultData, session.seqID+1)
}

func (s *Server) handleCloseStmt(session *Session, payload []byte) error {
    stmtID, _, err := s.codec.DecodeInt64(payload)
    if err != nil {
        return err
    }

    if err := s.handler.HandleClose(uint32(stmtID)); err != nil {
        return err
    }

    return WriteMessage(session.conn, MsgCloseStmt, []byte{}, session.seqID+1)
}

func (s *Server) handleBeginTx(session *Session) error {
    txID, err := s.handler.HandleBeginTx()
    if err != nil {
        return err
    }

    response := s.codec.EncodeInt64(int64(txID))
    return WriteMessage(session.conn, MsgBeginTx, response, session.seqID+1)
}

func (s *Server) handleCommitTx(session *Session, payload []byte) error {
    txID, _, err := s.codec.DecodeInt64(payload)
    if err != nil {
        return err
    }

    if err := s.handler.HandleCommitTx(uint64(txID)); err != nil {
        return err
    }

    return WriteMessage(session.conn, MsgCommitTx, []byte{}, session.seqID+1)
}

func (s *Server) handleRollbackTx(session *Session, payload []byte) error {
    txID, _, err := s.codec.DecodeInt64(payload)
    if err != nil {
        return err
    }

    if err := s.handler.HandleRollbackTx(uint64(txID)); err != nil {
        return err
    }

    return WriteMessage(session.conn, MsgRollbackTx, []byte{}, session.seqID+1)
}

func (s *Server) handlePing(session *Session) error {
    return WriteMessage(session.conn, MsgPong, []byte{}, session.seqID+1)
}

func (s *Server) sendError(session *Session, err error) {
    errData := s.codec.EncodeString(err.Error())
    WriteMessage(session.conn, MsgError, errData, session.seqID+1)
}

type Session struct {
    conn   net.Conn
    codec  *Codec
    seqID  uint32
    connID int32
}
