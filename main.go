package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/kelvinwambua/nimbledb/engine"
	"github.com/kelvinwambua/nimbledb/network"
	"github.com/kelvinwambua/nimbledb/repl"
	"github.com/kelvinwambua/nimbledb/sql"
)

func main() {
	mode := flag.String("mode", "repl", "Run mode: repl or server")
	addr := flag.String("addr", ":5432", "Server address (server mode only)")
	dataDir := flag.String("data", "./data", "Data directory")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatal("Failed to create data directory:", err)
	}

	switch *mode {
	case "repl":
		r := repl.NewREPL(*dataDir)
		r.Start()

	case "server":
		eng := engine.NewEngine(*dataDir)
		handler := &ServerHandler{engine: eng}
		server := network.NewServer(*addr, handler)

		fmt.Printf("Starting server on %s\n", *addr)
		if err := server.Start(); err != nil {
			log.Fatal("Server failed to start:", err)
		}

		select {}

	default:
		fmt.Println("Unknown mode:", *mode)
		flag.Usage()
	}
}

type ServerHandler struct {
	engine *engine.Engine
}

func (h *ServerHandler) HandleQuery(query string) ([]string, [][]interface{}, error) {
	parser := sql.NewParser(query)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, nil, err
	}
	return h.engine.Execute(stmt)
}

func (h *ServerHandler) HandlePrepare(query string) (uint32, error) {
	return 0, fmt.Errorf("prepared statements not yet implemented")
}

func (h *ServerHandler) HandleExecute(stmtID uint32, params []interface{}) ([]string, [][]interface{}, error) {
	return nil, nil, fmt.Errorf("prepared statements not yet implemented")
}

func (h *ServerHandler) HandleClose(stmtID uint32) error {
	return nil
}

func (h *ServerHandler) HandleBeginTx() (uint64, error) {
	return 0, fmt.Errorf("transactions not yet implemented")
}

func (h *ServerHandler) HandleCommitTx(txID uint64) error {
	return fmt.Errorf("transactions not yet implemented")
}

func (h *ServerHandler) HandleRollbackTx(txID uint64) error {
	return fmt.Errorf("transactions not yet implemented")
}
