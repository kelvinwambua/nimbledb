# NimbleDB - A Simple Relational Database Management System

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A lightweight, RDBMS implementation in Go that demonstrates core database concepts including storage management, query optimization, and client-server networking.

## Table of Contents

- [Quick Start](#-quick-start)
- [Architecture Overview](#-architecture-overview)
- [Storage Engine](#-storage-engine)
- [Query Processing](#-query-processing)
- [Network Layer](#-network-layer)
- [Features](#-features)
- [Testing Guide](#-testing-guide)
- [Implementation Details](#-implementation-details)

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/kelvinwambua/nimbledb.git
cd nimbledb

# Build the project
go build -o nimbledb cmd/main.go

# Run in REPL mode
./nimbledb -mode repl

# Or run as a server
./nimbledb -mode server -addr :5432
```

### First Steps

```sql
-- Create a table
CREATE TABLE users (id INT NOT NULL, name VARCHAR(100) NOT NULL, email VARCHAR(255), age INT, PRIMARY KEY (id));

-- Insert data
INSERT INTO users VALUES (1, 'Kelvin Wambua', 'kelvin@email.com', 20);
INSERT INTO users VALUES (2, 'Mtume Owino', 'mtume@email.com', 21);

-- Query data
SELECT * FROM users WHERE age > 20;

-- Update records
UPDATE users SET age = 29 WHERE id = 1;

-- Delete records
DELETE FROM users WHERE id = 2;
```

## Architecture Overview

NimbleDB follows a layered architecture inspired by traditional RDBMS design:

<img width="414" height="859" alt="Architecture" src="https://github.com/user-attachments/assets/9d87910b-e7d8-480a-ad8d-5fa36d3cbb88" />

## Storage Engine

The storage engine implements a page-based architecture with slotted page design, inspired by concepts from "Database Internals" by Alex Petrov.

### Page Structure

<img width="545" height="626" alt="image" src="https://github.com/user-attachments/assets/2f79887a-0521-48af-8351-0e0a3995e6a7" />

### Key Components

#### 1. **Pager** (`storage/pager.go`)

- Manages physical disk I/O
- Allocates and deallocates pages
- Maintains page directory
- Thread-safe with RWMutex

```go
type Pager struct {
    file     *os.File
    numPages uint32
    mu       sync.RWMutex
}
```

#### 2. **Page Layout** (`storage/page.go`)

- Fixed 4KB pages
- Slotted page design
- Header contains metadata
- Supports insert, update, delete operations

**Page Types:**

- `PageTypeTable`: Stores table data
- `PageTypeIndex`: Stores index nodes
- `PageTypeFree`: Available for allocation

#### 3. **Tuple Storage** (`storage/tuple.go`)

Supports multiple data types with efficient serialization:

| Data Type | Storage Size | Format                       |
| --------- | ------------ | ---------------------------- |
| INT       | 8 bytes      | Little-endian int64          |
| VARCHAR   | 2 + N bytes  | Length prefix + UTF-8 string |
| BOOL      | 1 byte       | 0 or 1                       |
| FLOAT     | 8 bytes      | IEEE 754 double              |
| DATE      | 8 bytes      | Unix timestamp               |

#### 4. **Table Management** (`storage/table.go`)

- Linked list of pages
- Full table scans
- Filter support
- CRUD operations

**Table Structure:**

## Query Processing

### SQL Parser (`sql/parser.go`)

The parser uses a simple tokenization approach:

```
SQL Query -> Tokenizer -> Token Stream -> Parser -> AST
```

**Example:**

```sql
SELECT name, age FROM users WHERE age > 25
```

**Tokenization:**

```
[SELECT] [name] [,] [age] [FROM] [users] [WHERE] [age] [>] [25]
```

**Abstract Syntax Tree:**

```
SelectStmt
├── Columns: ["name", "age"]
├── TableName: "users"
└── Where: BinaryExpr
    ├── Left: ColumnExpr("age")
    ├── Op: OpGreaterThan
    └── Right: LiteralExpr(25)
```

### Query Optimizer (`query/optimizer.go`)

Implements cost-based optimization.

#### 1. **Index Selection**

```
Decision Tree:
    Has WHERE clause?
        ├─ No  -> Table Scan
        └─ Yes
            ├─ Index available on column?
            │   ├─ Yes _> Compare costs
            │   │   └─ Index Scan vs Table Scan
            │   └─ No -> Table Scan
```

**Cost Model:**

```
Table Scan Cost = RowCount × 0.1
Index Scan Cost = IndexHeight × 10 + EstimatedRows × 0.05
```

#### 2. **Join Optimization**

Supports two join algorithms:

**Nested Loop Join:**

```
for each row r in R:
    for each row s in S:
        if join_condition(r, s):
            output(r, s)

Cost = Cost(R) + Cardinality(R) × Cost(S)
```

**Hash Join (for equi-joins):**

```
Phase 1 (Build):
    hash_table = {}
    for each row r in R:
        hash_table[r.join_key] = r

Phase 2 (Probe):
    for each row s in S:
        if s.join_key in hash_table:
            output(hash_table[s.join_key], s)

Cost = Cost(R) + Cost(S) + (Card(R) + Card(S)) × 0.01
```

#### 3. **Selectivity Estimation**

```
OpEqual:        1 / distinct_values
OpNotEqual:     1 - (1 / distinct_values)
OpLessThan:     0.33 (default assumption)
OpGreaterThan:  0.33 (default assumption)
OpAnd:          selectivity(left) × selectivity(right)
OpOr:           sel(L) + sel(R) - sel(L) × sel(R)
```

### Query Execution Plans

**Example Plan Visualization:**

```sql
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.age > 30
ORDER BY o.total DESC
LIMIT 10
```

**Execution Plan:**

```
Limit(10) [cost=5.2]
  └─ Sort(total DESC) [cost=52.1]
      └─ Project(name, total) [cost=48.3]
          └─ HashJoin [cost=48.0]
              ├─ Filter(age > 30) [cost=12.5]
              │   └─ TableScan(users) [cost=10.0]
              └─ TableScan(orders) [cost=20.0]
```

### Query Rewriter (`query/rewriter.go`)

Applies algebraic transformations:

1. **Push Down Negation** (De Morgan's Laws)

```
NOT (a AND b) _> (NOT a) OR (NOT b)
NOT (a OR b)  -> (NOT a) AND (NOT b)
```

2. **Eliminate Double Negation**

```
NOT (NOT a) -> a
```

3. **Convert to CNF** (Conjunctive Normal Form)

```
(a OR b) AND c -> (a AND c) OR (b AND c)
```

## Network Layer

### Protocol Design

Custom binary protocol with message framing:

<img width="538" height="387" alt="image" src="https://github.com/user-attachments/assets/8db37119-d382-4726-8180-5815e3689103" />

### Message Types

| Type         | Code      | Description                |
| ------------ | --------- | -------------------------- |
| Handshake    | 0x00      | Connection initialization  |
| Query        | 0x01      | SQL query request          |
| QueryResult  | 0x02      | Query response             |
| PreparedStmt | 0x03      | Prepare statement          |
| ExecuteStmt  | 0x04      | Execute prepared statement |
| BeginTx      | 0x06      | Start transaction          |
| CommitTx     | 0x07      | Commit transaction         |
| Error        | 0x09      | Error response             |
| Ping/Pong    | 0x0A/0x0B | Keep-alive                 |

### Codec (`network/codec.go`)

Efficient binary encoding:

**Type Tags:**

- 0x00: NULL
- 0x01: INT64
- 0x02: STRING
- 0x03: BOOL
- 0x04: FLOAT64
- 0x05: TIMESTAMP

### Server Architecture (`network/server.go`)

<img width="594" height="781" alt="image" src="https://github.com/user-attachments/assets/615445e9-3ef4-4b30-b230-227377fb0367" />

**Concurrency Model:**

- One goroutine per connection
- Thread-safe engine with mutex protection
- Atomic sequence ID generation
- Graceful shutdown with WaitGroup

## Features

### B-Tree Indexing (`index/btree.go`)

**Properties:**

- Order: 4 (max 7 keys per node)
- Self-balancing
- O(log n) search, insert operations
- Range scan support

**Operations:**

- `Insert(key, pageID, slotID)`: Add index entry
- `Search(key)`: Find record location
- `RangeScan(start, end)`: Range queries

### Constraint Support

1. **PRIMARY KEY**
   - Uniqueness enforced
   - Automatic index creation
   - Single or composite keys

2. **UNIQUE**
   - Prevents duplicates
   - Hash-based validation

3. **NOT NULL**
   - Column-level constraint
   - Enforced at insert/update

### Integration Testing

#### 1. REPL Mode Testing

```sql
-- Create test database
CREATE TABLE products ( id INT NOT NULL,name VARCHAR(100) NOT NULL,price FLOAT,stock INT,PRIMARY KEY (id));

-- Insert test data
INSERT INTO products VALUES (1, 'Laptop', 999.99, 10);
INSERT INTO products VALUES (2, 'Mouse', 29.99, 50);
INSERT INTO products VALUES (3, 'Keyboard', 79.99, 30);

-- Test queries
SELECT * FROM products WHERE price < 100;
SELECT name, stock FROM products ORDER BY stock DESC;
UPDATE products SET stock = 45 WHERE id = 2;
DELETE FROM products WHERE id = 3;
```

#### 2. Client-Server Testing

**Terminal 1 - Start Server:**

```bash
./nimbledb -mode server -addr :5432
```

**Terminal 2 - Client Code:**

```go
package main

import (
    "fmt"
    "github.com/kelvinwambua/nimbledb/network"
)

func main() {
    client := network.NewClient("localhost:5432")
    if err := client.Connect(); err != nil {
        panic(err)
    }
    defer client.Close()

    // Create table
    _, _, err := client.Query(`
        CREATE TABLE test (id INT, name VARCHAR(50), PRIMARY KEY (id))
    `)
    if err != nil {
        panic(err)
    }

    // Insert data
    client.Query("INSERT INTO test VALUES (1, 'Alice')")
    client.Query("INSERT INTO test VALUES (2, 'Bob')")

    // Query
    cols, rows, err := client.Query("SELECT * FROM test")
    if err != nil {
        panic(err)
    }

    fmt.Println("Columns:", cols)
    for _, row := range rows {
        fmt.Println("Row:", row)
    }
}
```

#### 3. Performance Testing

```go
// Benchmark insert performance
func BenchmarkInsert(b *testing.B) {
    eng := engine.NewEngine("./testdata")
    defer eng.Close()

    // Create table
    parser := sql.NewParser("CREATE TABLE bench (id INT, val INT, PRIMARY KEY (id))")
    stmt, _ := parser.Parse()
    eng.Execute(stmt)

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        query := fmt.Sprintf("INSERT INTO bench VALUES (%d, %d)", i, i*2)
        parser := sql.NewParser(query)
        stmt, _ := parser.Parse()
        eng.Execute(stmt)
    }
}
```

### Test Scenarios

#### Scenario 1: Join Operations

```sql
-- Setup
CREATE TABLE customers (id INT, name VARCHAR(100), PRIMARY KEY (id));
CREATE TABLE orders (order_id INT, customer_id INT, amount FLOAT, PRIMARY KEY (order_id));

INSERT INTO customers VALUES (1, 'Alice');
INSERT INTO customers VALUES (2, 'Bob');
INSERT INTO orders VALUES (101, 1, 150.00);
INSERT INTO orders VALUES (102, 1, 200.00);
INSERT INTO orders VALUES (103, 2, 75.00);

-- Test join
SELECT name, amount FROM customers JOIN orders ON customers.id = orders.customer_id;
```

**Expected Output:**

```
+-------+--------+
| name  | amount |
+-------+--------+
| Alice | 150.00 |
| Alice | 200.00 |
+-------+--------+
```

#### Scenario 2: Index Performance

```sql
-- Create table with index
CREATE TABLE users (
    id INT NOT NULL,
    email VARCHAR(255),
    age INT,
    PRIMARY KEY (id)
);

-- Insert bulk data (simulate with loop)
-- INSERT INTO users VALUES (...) x 1000

-- Compare query plans
EXPLAIN SELECT * FROM users WHERE id = 500;
-- Should use index scan

EXPLAIN SELECT * FROM users WHERE age = 25;
-- Should use table scan (no index on age)
```

## Implementation Details

### Memory Management

- Fixed page size (4KB) reduces fragmentation
- Slot directory allows efficient space reuse
- Dead tuple cleanup on delete operations

### Concurrency Control

Current implementation uses coarse-grained locking:

```go
type Table struct {
    mu sync.RWMutex
    // ...
}

func (t *Table) Insert(tuple *Tuple) error {
    t.mu.Lock()
    defer t.mu.Unlock()
    // ... insert logic
}
```

### Error Handling

Consistent error propagation:

```go
if err := page.InsertRecord(data); err != nil {
    return fmt.Errorf("failed to insert record: %w", err)
}
```

### File Layout

```
data/
├── metadata.db      # System catalog
├── table_users.db   # User table data
├── table_orders.db  # Orders table data
└── index_users_pk.db # Primary key index
```

## Limitations & Future Work

### Current Limitations

1. **No WAL (Write-Ahead Logging)**
   - No crash recovery
   - No point-in-time recovery

2. **No MVCC**
   - Coarse-grained locking
   - Limited concurrency

3. **Simple Query Optimizer**
   - Basic cost model
   - No join reordering
   - No statistics maintenance

4. **Limited Data Types**
   - No DECIMAL, BLOB, or JSON
   - No user-defined types

### Potential Improvements

1. **Add Transaction Support**
   - ACID properties
   - Isolation levels
   - Deadlock detection

2. **Implement Buffer Pool**
   - Page caching
   - LRU eviction
   - Dirty page management

3. **Advanced Indexing**
   - Hash indexes
   - Full-text search
   - Spatial indexes

4. **Query Optimization**
   - Statistics collection
   - Histogram-based estimation
   - Join reordering

## References

- **Database Internals** by Alex Petrov - Page layout, B-Tree implementation
- **Architecture of a Database System** - Query processing pipeline
- **PostgreSQL Documentation** - Protocol design inspiration
- **SQLite Source Code** - REPL implementation patterns

## License

MIT License - see LICENSE file for details

## Author

Built by [Kelvin Wambua](https://github.com/kelvinwambua) as part of the Pesapal Junior Dev Challenge '26

---
