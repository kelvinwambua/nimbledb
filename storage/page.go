package storage

import (
	"encoding/binary"
	"errors"
)

const (
	PageSize       = 4096
	PageHeaderSize = 25
	SlotSize       = 4
)

type PageType uint8

const (
	PageTypeTable PageType = 1
	PageTypeIndex PageType = 2
	PageTypeFree  PageType = 3
)

type Page struct {
	header PageHeader
	data   [PageSize]byte
}

type PageHeader struct {
	PageID     uint32
	PageType   PageType
	FreeSpace  uint16
	SlotCount  uint16
	NextPageID uint32
	PrevPageID uint32
	LSN        uint64
}

func NewPage(pageID uint32, pageType PageType) *Page {
	p := &Page{}
	p.header.PageID = pageID
	p.header.PageType = pageType
	p.header.FreeSpace = PageSize - PageHeaderSize
	p.header.SlotCount = 0
	p.header.NextPageID = 0
	p.header.PrevPageID = 0
	p.header.LSN = 0
	p.writeHeader()
	return p
}

func (p *Page) writeHeader() {
	binary.LittleEndian.PutUint32(p.data[0:4], p.header.PageID)
	p.data[4] = byte(p.header.PageType)
	binary.LittleEndian.PutUint16(p.data[5:7], p.header.FreeSpace)
	binary.LittleEndian.PutUint16(p.data[7:9], p.header.SlotCount)
	binary.LittleEndian.PutUint32(p.data[9:13], p.header.NextPageID)
	binary.LittleEndian.PutUint32(p.data[13:17], p.header.PrevPageID)
	binary.LittleEndian.PutUint64(p.data[17:25], p.header.LSN)
}

func (p *Page) readHeader() {
	p.header.PageID = binary.LittleEndian.Uint32(p.data[0:4])
	p.header.PageType = PageType(p.data[4])
	p.header.FreeSpace = binary.LittleEndian.Uint16(p.data[5:7])
	p.header.SlotCount = binary.LittleEndian.Uint16(p.data[7:9])
	p.header.NextPageID = binary.LittleEndian.Uint32(p.data[9:13])
	p.header.PrevPageID = binary.LittleEndian.Uint32(p.data[13:17])
	p.header.LSN = binary.LittleEndian.Uint64(p.data[17:25])
}

func (p *Page) getSlotOffset(slotID uint16) uint16 {
	slotPos := PageHeaderSize + (slotID * SlotSize)
	return binary.LittleEndian.Uint16(p.data[slotPos : slotPos+2])
}

func (p *Page) getSlotLength(slotID uint16) uint16 {
	slotPos := PageHeaderSize + (slotID * SlotSize)
	return binary.LittleEndian.Uint16(p.data[slotPos+2 : slotPos+4])
}

func (p *Page) setSlot(slotID, offset, length uint16) {
	slotPos := PageHeaderSize + (slotID * SlotSize)
	binary.LittleEndian.PutUint16(p.data[slotPos:slotPos+2], offset)
	binary.LittleEndian.PutUint16(p.data[slotPos+2:slotPos+4], length)
}

func (p *Page) InsertRecord(record []byte) (uint16, error) {
	recordLen := uint16(len(record))
	requiredSpace := recordLen + SlotSize

	if p.header.FreeSpace < requiredSpace {
		return 0, errors.New("insufficient space in page")
	}

	slotAreaEnd := PageHeaderSize + (p.header.SlotCount * SlotSize)
	dataAreaStart := uint16(PageSize)

	for i := uint16(0); i < p.header.SlotCount; i++ {
		offset := p.getSlotOffset(i)
		if offset < dataAreaStart {
			dataAreaStart = offset
		}
	}

	if uint16(slotAreaEnd)+SlotSize > dataAreaStart-recordLen {
		return 0, errors.New("page fragmented, compaction needed")
	}

	newOffset := dataAreaStart - recordLen
	copy(p.data[newOffset:newOffset+recordLen], record)

	slotID := p.header.SlotCount
	p.setSlot(slotID, newOffset, recordLen)

	p.header.SlotCount++
	p.header.FreeSpace -= requiredSpace
	p.writeHeader()

	return slotID, nil
}

func (p *Page) GetRecord(slotID uint16) ([]byte, error) {
	if slotID >= p.header.SlotCount {
		return nil, errors.New("invalid slot ID")
	}

	offset := p.getSlotOffset(slotID)
	length := p.getSlotLength(slotID)

	if length == 0 {
		return nil, errors.New("record deleted")
	}

	record := make([]byte, length)
	copy(record, p.data[offset:offset+length])
	return record, nil
}

func (p *Page) DeleteRecord(slotID uint16) error {
	if slotID >= p.header.SlotCount {
		return errors.New("invalid slot ID")
	}

	length := p.getSlotLength(slotID)
	p.setSlot(slotID, 0, 0)
	p.header.FreeSpace += length
	p.writeHeader()

	return nil
}

func (p *Page) UpdateRecord(slotID uint16, record []byte) error {
	if slotID >= p.header.SlotCount {
		return errors.New("invalid slot ID")
	}

	oldLength := p.getSlotLength(slotID)
	newLength := uint16(len(record))

	if newLength > oldLength {
		if err := p.DeleteRecord(slotID); err != nil {
			return err
		}
		_, err := p.InsertRecord(record)
		return err
	}

	offset := p.getSlotOffset(slotID)
	copy(p.data[offset:offset+newLength], record)
	p.setSlot(slotID, offset, newLength)
	p.header.FreeSpace += oldLength - newLength
	p.writeHeader()

	return nil
}

func (p *Page) ToBytes() []byte {
	p.writeHeader()
	return p.data[:]
}

func PageFromBytes(data []byte) (*Page, error) {
	if len(data) != PageSize {
		return nil, errors.New("invalid page size")
	}

	p := &Page{}
	copy(p.data[:], data)
	p.readHeader()
	return p, nil
}
