package repl

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/kelvinwambua/nimbledb/engine"
	"github.com/kelvinwambua/nimbledb/sql"
)

type REPL struct {
	engine *engine.Engine
	reader *bufio.Reader
}

func NewREPL(dataDir string) *REPL {
	return &REPL{
		engine: engine.NewEngine(dataDir),
		reader: bufio.NewReader(os.Stdin),
	}
}

func (r *REPL) Start() {
	fmt.Println("Welcome to NimbleRDBMS!")
	fmt.Println("Type SQL commands or 'exit' to quit.")
	fmt.Println()

	for {
		fmt.Print("rdbms> ")

		input, err := r.reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			fmt.Println("Goodbye!")
			r.engine.Close()
			break
		}

		if strings.ToLower(input) == "help" {
			r.printHelp()
			continue
		}

		if !strings.HasSuffix(input, ";") {
			input += ";"
		}

		if err := r.execute(input); err != nil {
			fmt.Printf("Error: %v\n", err)
		}

		fmt.Println()
	}
}

func (r *REPL) execute(query string) error {
	parser := sql.NewParser(query)
	stmt, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	columns, rows, err := r.engine.Execute(stmt)
	if err != nil {
		return fmt.Errorf("execution error: %w", err)
	}

	r.printResults(columns, rows)
	return nil
}

func (r *REPL) printResults(columns []string, rows [][]interface{}) {
	if len(rows) == 0 {
		fmt.Println("Query executed successfully. No rows returned.")
		return
	}

	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col)
	}

	for _, row := range rows {
		for i, val := range row {
			valStr := fmt.Sprintf("%v", val)
			if len(valStr) > colWidths[i] {
				colWidths[i] = len(valStr)
			}
		}
	}

	separator := "+"
	for _, width := range colWidths {
		separator += strings.Repeat("-", width+2) + "+"
	}

	fmt.Println(separator)

	header := "|"
	for i, col := range columns {
		header += fmt.Sprintf(" %-*s |", colWidths[i], col)
	}
	fmt.Println(header)

	fmt.Println(separator)

	for _, row := range rows {
		rowStr := "|"
		for i, val := range row {
			valStr := fmt.Sprintf("%v", val)
			if val == nil {
				valStr = "NULL"
			}
			rowStr += fmt.Sprintf(" %-*s |", colWidths[i], valStr)
		}
		fmt.Println(rowStr)
	}

	fmt.Println(separator)
	fmt.Printf("%d row(s) returned\n", len(rows))
}

func (r *REPL) printHelp() {
	fmt.Println("Available commands:")
	fmt.
		Println("  CREATE TABLE <name> (<column definitions>)")
	fmt.Println("  INSERT INTO <table> VALUES (<values>)")
	fmt.Println("  SELECT <columns> FROM <table> [WHERE <condition>]")
	fmt.Println("  UPDATE <table> SET <assignments> [WHERE <condition>]")
	fmt.Println("  DELETE FROM <table> [WHERE <condition>]")
	fmt.Println("  DROP TABLE <table>")
	fmt.Println("  exit/quit - Exit the REPL")
	fmt.Println("  help - Show this help message")
}
