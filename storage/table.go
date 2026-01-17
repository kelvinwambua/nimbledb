package storage

import (
	"errors"
	"fmt"
	"sync"
)

type Table struct {
	Name       string
	Columns    []Column
	Pager      *Pager
	RootPageID uint32
	mu         sync.RWMutex
}

func NewTable(name string, columns []Column, pager *Pager) (*Table, error) {
	rootPage, err := pager.AllocatePage(PageTypeTable)
	if err != nil {
		return nil, err
	}

	return &Table{
		Name:       name,
		Columns:    columns,
		Pager:      pager,
		RootPageID: rootPage.header.PageID,
	}, nil
}

func (t *Table) Insert(tuple *Tuple) (uint32, uint16, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(tuple.values) != len(t.Columns) {
		return 0, 0, errors.New("column count mismatch")
	}

	data := tuple.Serialize()

	page, err := t.Pager.ReadPage(t.RootPageID)
	if err != nil {
		return 0, 0, err
	}

	currentPageID := t.RootPageID
	for {
		slotID, err := page.InsertRecord(data)
		if err == nil {
			writeErr := t.Pager.WritePage(page)
			return currentPageID, slotID, writeErr
		}

		if page.header.NextPageID == 0 {
			newPage, err := t.Pager.AllocatePage(PageTypeTable)
			if err != nil {
				return 0, 0, err
			}

			page.header.NextPageID = newPage.header.PageID
			newPage.header.PrevPageID = currentPageID

			if err := t.Pager.WritePage(page); err != nil {
				return 0, 0, err
			}

			page = newPage
			currentPageID = newPage.header.PageID
		} else {
			page, err = t.Pager.ReadPage(page.header.NextPageID)
			if err != nil {
				return 0, 0, err
			}
			currentPageID = page.header.PageID
		}
	}
}

func (t *Table) Scan(filter func(*Tuple) bool) ([]*Tuple, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	results := make([]*Tuple, 0)
	currentPageID := t.RootPageID

	for currentPageID != 0 {
		page, err := t.Pager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}

		for slotID := uint16(0); slotID < page.header.SlotCount; slotID++ {
			record, err := page.GetRecord(slotID)
			if err != nil {
				continue
			}

			tuple := NewTuple(t.Columns)
			if err := tuple.Deserialize(record); err != nil {
				continue
			}

			if filter == nil || filter(tuple) {
				results = append(results, tuple)
			}
		}

		currentPageID = page.header.NextPageID
	}

	return results, nil
}

func (t *Table) Delete(filter func(*Tuple) bool) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	deleted := 0
	currentPageID := t.RootPageID

	for currentPageID != 0 {
		page, err := t.Pager.ReadPage(currentPageID)
		if err != nil {
			return deleted, err
		}

		for slotID := uint16(0); slotID < page.header.SlotCount; slotID++ {
			record, err := page.GetRecord(slotID)
			if err != nil {
				continue
			}

			tuple := NewTuple(t.Columns)
			if err := tuple.Deserialize(record); err != nil {
				continue
			}

			if filter(tuple) {
				if err := page.DeleteRecord(slotID); err == nil {
					deleted++
				}
			}
		}

		if err := t.Pager.WritePage(page); err != nil {
			return deleted, err
		}

		currentPageID = page.header.NextPageID
	}

	return deleted, nil
}

func (t *Table) Update(filter func(*Tuple) bool, updater func(*Tuple) error) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	updated := 0
	currentPageID := t.RootPageID

	for currentPageID != 0 {
		page, err := t.Pager.ReadPage(currentPageID)
		if err != nil {
			return updated, err
		}

		for slotID := uint16(0); slotID < page.header.SlotCount; slotID++ {
			record, err := page.GetRecord(slotID)
			if err != nil {
				continue
			}

			tuple := NewTuple(t.Columns)
			if err := tuple.Deserialize(record); err != nil {
				continue
			}

			if filter(tuple) {
				if err := updater(tuple); err != nil {
					return updated, err
				}

				newData := tuple.Serialize()
				if err := page.UpdateRecord(slotID, newData); err != nil {
					return updated, err
				}
				updated++
			}
		}

		if err := t.Pager.WritePage(page); err != nil {
			return updated, err
		}

		currentPageID = page.header.NextPageID
	}

	return updated, nil
}

func (t *Table) Print() error {
	tuples, err := t.Scan(nil)
	if err != nil {
		return err
	}

	for _, col := range t.Columns {
		fmt.Printf("%-15s ", col.Name)
	}
	fmt.Println()

	for range t.Columns {
		fmt.Print("--------------- ")
	}
	fmt.Println()

	for _, tuple := range tuples {
		for _, val := range tuple.values {
			fmt.Printf("%-15v ", val)
		}
		fmt.Println()
	}

	return nil
}
