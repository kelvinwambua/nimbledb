package storage

import (
	"errors"
	"os"
	"sync"
)

type Pager struct {
	file     *os.File
	numPages uint32
	mu       sync.RWMutex
}

func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	numPages := uint32(stat.Size() / PageSize)

	return &Pager{
		file:     file,
		numPages: numPages,
	}, nil
}

func (p *Pager) ReadPage(pageID uint32) (*Page, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if pageID >= p.numPages {
		return nil, errors.New("page ID out of bounds")
	}

	data := make([]byte, PageSize)
	offset := int64(pageID) * PageSize
	_, err := p.file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	return PageFromBytes(data)
}

func (p *Pager) WritePage(page *Page) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset := int64(page.header.PageID) * PageSize
	_, err := p.file.WriteAt(page.ToBytes(), offset)
	if err != nil {
		return err
	}

	if page.header.PageID >= p.numPages {
		p.numPages = page.header.PageID + 1
	}

	return p.file.Sync()
}

func (p *Pager) AllocatePage(pageType PageType) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pageID := p.numPages
	if pageID == 0 {
		pageID = 1 // Start from page 1, reserve page 0
	}

	page := NewPage(pageID, pageType)

	offset := int64(pageID) * PageSize
	_, err := p.file.WriteAt(page.ToBytes(), offset)
	if err != nil {
		return nil, err
	}

	if pageID >= p.numPages {
		p.numPages = pageID + 1
	}

	return page, p.file.Sync()
}

func (p *Pager) Close() error {
	return p.file.Close()
}

func (p *Pager) NumPages() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numPages
}
