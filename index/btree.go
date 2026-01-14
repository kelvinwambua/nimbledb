package index

import (
	"bytes"

	"sync"
)

const (
	BTreeOrder = 4
)

type BTreeNode struct {
	isLeaf   bool
	keys     []interface{}
	values   [][]uint32
	children []*BTreeNode
	next     *BTreeNode
}

type BTree struct {
	root    *BTreeNode
	compare func(interface{}, interface{}) int
	mu      sync.RWMutex
}

func NewBTree(compare func(interface{}, interface{}) int) *BTree {
	return &BTree{
		root: &BTreeNode{
			isLeaf: true,
			keys:   make([]interface{}, 0),
			values: make([][]uint32, 0),
		},
		compare: compare,
	}
}

func (bt *BTree) Insert(key interface{}, pageID, slotID uint32) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if len(bt.root.keys) == 2*BTreeOrder-1 {
		oldRoot := bt.root
		bt.root = &BTreeNode{
			isLeaf:   false,
			keys:     make([]interface{}, 0),
			children: []*BTreeNode{oldRoot},
		}
		bt.splitChild(bt.root, 0)
	}

	return bt.insertNonFull(bt.root, key, pageID, slotID)
}

func (bt *BTree) insertNonFull(node *BTreeNode, key interface{}, pageID, slotID uint32) error {
	i := len(node.keys) - 1

	if node.isLeaf {
		node.keys = append(node.keys, nil)
		node.values = append(node.values, nil)

		for i >= 0 && bt.compare(key, node.keys[i]) < 0 {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}

		node.keys[i+1] = key
		node.values[i+1] = []uint32{pageID, slotID}
		return nil
	}

	for i >= 0 && bt.compare(key, node.keys[i]) < 0 {
		i--
	}
	i++

	if len(node.children[i].keys) == 2*BTreeOrder-1 {
		bt.splitChild(node, i)
		if bt.compare(key, node.keys[i]) > 0 {
			i++
		}
	}

	return bt.insertNonFull(node.children[i], key, pageID, slotID)
}

func (bt *BTree) splitChild(parent *BTreeNode, index int) {
	fullChild := parent.children[index]
	newChild := &BTreeNode{
		isLeaf: fullChild.isLeaf,
		keys:   make([]interface{}, BTreeOrder-1),
		values: make([][]uint32, BTreeOrder-1),
	}

	copy(newChild.keys, fullChild.keys[BTreeOrder:])
	copy(newChild.values, fullChild.values[BTreeOrder:])

	if !fullChild.isLeaf {
		newChild.children = make([]*BTreeNode, BTreeOrder)
		copy(newChild.children, fullChild.children[BTreeOrder:])
		fullChild.children = fullChild.children[:BTreeOrder]
	}

	midKey := fullChild.keys[BTreeOrder-1]
	fullChild.keys = fullChild.keys[:BTreeOrder-1]
	fullChild.values = fullChild.values[:BTreeOrder-1]

	parent.keys = append(parent.keys, nil)
	copy(parent.keys[index+1:], parent.keys[index:])
	parent.keys[index] = midKey

	parent.children = append(parent.children, nil)
	copy(parent.children[index+2:], parent.children[index+1:])
	parent.children[index+1] = newChild
}

func (bt *BTree) Search(key interface{}) ([]uint32, bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	return bt.search(bt.root, key)
}

func (bt *BTree) search(node *BTreeNode, key interface{}) ([]uint32, bool) {
	i := 0
	for i < len(node.keys) && bt.compare(key, node.keys[i]) > 0 {
		i++
	}

	if i < len(node.keys) && bt.compare(key, node.keys[i]) == 0 {
		return node.values[i], true
	}

	if node.isLeaf {
		return nil, false
	}

	return bt.search(node.children[i], key)
}

func (bt *BTree) RangeScan(start, end interface{}) [][]uint32 {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	results := make([][]uint32, 0)
	bt.rangeScan(bt.root, start, end, &results)
	return results
}

func (bt *BTree) rangeScan(node *BTreeNode, start, end interface{}, results *[][]uint32) {
	if node == nil {
		return
	}

	i := 0
	for i < len(node.keys) {
		if !node.isLeaf {
			if bt.compare(start, node.keys[i]) <= 0 {
				bt.rangeScan(node.children[i], start, end, results)
			}
		}

		if bt.compare(node.keys[i], start) >= 0 && bt.compare(node.keys[i], end) <= 0 {
			*results = append(*results, node.values[i])
		}

		if bt.compare(node.keys[i], end) > 0 {
			return
		}

		i++
	}

	if !node.isLeaf {
		bt.rangeScan(node.children[i], start, end, results)
	}
}

func DefaultCompare(a, b interface{}) int {
	switch va := a.(type) {
	case int64:
		vb := b.(int64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		vb := b.(string)
		return bytes.Compare([]byte(va), []byte(vb))
	case float64:
		vb := b.(float64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	default:
		return 0
	}
}
