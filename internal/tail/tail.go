// Package tail 负责跟踪单个文件的新增内容。
package tail

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"time"
)

// Options 是 Tailer 的构造参数。
type Options struct {
	Path         string
	PollInterval time.Duration
	// StartOffset 是续读位置（字节）。0 表示从头读。
	StartOffset int64
}

// Tailer 按行读取文件，读到末尾后轮询等待新内容。
type Tailer struct {
	opts    Options
	onLine  func(string)
	onError func(error)
}

// New 构造 Tailer。回调不能为 nil。
func New(opts Options, onLine func(string), onError func(error)) *Tailer {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 200 * time.Millisecond
	}
	return &Tailer{opts: opts, onLine: onLine, onError: onError}
}

// Run 阻塞运行直到 ctx 结束。返回当前读取到的字节偏移。
//
// 末尾未换行的半行（pending）不计入返回的位点：它还没有作为完整行
// 输出过，下次运行时从行首重读。宁可重读一段，也不把截断的半行
// 当作记录发出去。
func (t *Tailer) Run(ctx context.Context) (int64, error) {
	f, err := os.Open(t.opts.Path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	offset := t.opts.StartOffset
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return offset, err
	}

	reader := bufio.NewReader(f)
	var pending []byte

	for {
		select {
		case <-ctx.Done():
			return offset - int64(len(pending)), ctx.Err()
		default:
		}

		chunk, err := reader.ReadBytes('\n')
		if len(chunk) > 0 {
			offset += int64(len(chunk))
			if chunk[len(chunk)-1] == '\n' {
				line := append(pending, chunk[:len(chunk)-1]...)
				pending = nil
				t.onLine(string(line))
			} else {
				// 末尾没有换行：可能是半行，先缓存，等下一轮补齐。
				pending = append(pending, chunk...)
			}
		}

		if errors.Is(err, io.EOF) {
			// 目前只做"等待新内容"，不检查文件是否被轮转或截断。
			time.Sleep(t.opts.PollInterval)
			continue
		}
		if err != nil {
			t.onError(err)
			return offset - int64(len(pending)), err
		}
	}
}
