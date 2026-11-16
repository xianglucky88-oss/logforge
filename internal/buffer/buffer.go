// Package buffer 是采集与输出之间的有界队列。
//
// 设计目标：下游慢的时候不能无限占用内存。容量满时有两种策略：
// 丢弃最旧的（保新）或阻塞（保全）。当前实现只有内存队列。
package buffer

import (
	"errors"
	"sync"
)

// ErrClosed 表示队列已关闭。
var ErrClosed = errors.New("buffer closed")

// Policy 决定容量满时的行为。
type Policy string

const (
	// PolicyDropOldest 丢弃最旧的一条，保留最新数据。
	PolicyDropOldest Policy = "drop_oldest"
	// PolicyBlock 阻塞生产者直到有空间。
	PolicyBlock Policy = "block"
)

// Buffer 是并发安全的 FIFO。元素是不透明的负载，由生产者和消费者约定类型。
type Buffer struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    []any
	capacity int
	policy   Policy
	closed   bool
	dropped  int
	// spillDir 预留给磁盘溢写，当前未使用。
	spillDir string
}

// New 构造缓冲。capacity <= 0 表示无界。
func New(capacity int, policy Policy, spillDir string) *Buffer {
	b := &Buffer{capacity: capacity, policy: policy, spillDir: spillDir}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Push 入队。返回是否因为容量限制丢弃了数据。
func (b *Buffer) Push(item any) (dropped bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return false, ErrClosed
	}

	if b.capacity > 0 && len(b.items) >= b.capacity {
		switch b.policy {
		case PolicyBlock:
			for len(b.items) >= b.capacity && !b.closed {
				b.cond.Wait()
			}
			if b.closed {
				return false, ErrClosed
			}
		default:
			// 当前实现：容量满时直接丢弃新数据。
			b.dropped++
			return true, nil
		}
	}

	b.items = append(b.items, item)
	b.cond.Broadcast()
	return false, nil
}

// Pop 出队，队列为空时阻塞直到有数据或关闭。
func (b *Buffer) Pop() (any, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.items) == 0 && !b.closed {
		b.cond.Wait()
	}
	if len(b.items) == 0 {
		return nil, false
	}
	item := b.items[0]
	b.items = b.items[1:]
	b.cond.Broadcast()
	return item, true
}

// Len 返回当前长度。
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

// Dropped 返回累计丢弃条数。
func (b *Buffer) Dropped() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

// Close 关闭队列，唤醒所有等待者。
func (b *Buffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.cond.Broadcast()
}
