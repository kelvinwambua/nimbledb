package index

import (
	"errors"
	"fmt"
)

type IndexType int

const (
	IndexTypeBTree IndexType = iota
	IndexTypeHash
)

type Index struct {
	name       string
	tableName  string
	Columns    []string // Capitalize to export
	indexType  IndexType
	isUnique   bool
	isPrimary  bool
	btree      *BTree
	uniqueKeys map[string]bool
}

func NewIndex(name, tableName string, columns []string, isUnique, isPrimary bool) *Index {
	return &Index{
		name:       name,
		tableName:  tableName,
		Columns:    columns, // Capitalize
		indexType:  IndexTypeBTree,
		isUnique:   isUnique,
		isPrimary:  isPrimary,
		btree:      NewBTree(DefaultCompare),
		uniqueKeys: make(map[string]bool),
	}
}

func (idx *Index) Insert(key interface{}, pageID, slotID uint32) error {
	if idx.isUnique || idx.isPrimary {
		keyStr := fmt.Sprintf("%v", key)
		if idx.uniqueKeys[keyStr] {
			return errors.New("unique constraint violation")
		}
		idx.uniqueKeys[keyStr] = true
	}
	return idx.btree.Insert(key, pageID, slotID)
}

func (idx *Index) Search(key interface{}) ([]uint32, bool) {
	return idx.btree.Search(key)
}

func (idx *Index) RangeScan(start, end interface{}) [][]uint32 {
	return idx.btree.RangeScan(start, end)
}

func (idx *Index) Delete(key interface{}) error {
	if idx.isUnique || idx.isPrimary {
		keyStr := fmt.Sprintf("%v", key)
		delete(idx.uniqueKeys, keyStr)
	}
	return nil
}
