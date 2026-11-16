// 磁盘溢写的帧格式。
//
// 一帧的二进制布局（大端）：
//
//	+---------+---------+----------------------+---------+
//	| magic 4 | len 4   | body (len 字节)      | crc32 4 |
//	+---------+---------+----------------------+---------+
//
// body 内部：
//
//	seq(8) | attempts(4) | dead(1) | payload_len(4) | payload(JSON)
//
// CRC32(IEEE) 覆盖整个 body。len 只描述 body，不含 magic/len/crc 本身，
// 读到损坏帧时可以靠 magic 做字节级重同步，而不是丢掉整个段文件。
package buffer

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// frameMagic 是每一帧开头的魔数。
var frameMagic = [4]byte{'L', 'F', 'S', '1'}

const (
	headerLen = 8 + 4 + 1 + 4 // seq + attempts + dead + payload_len
	crcLen    = 4
	// maxFramePayload 限制单条记录序列化后的大小，
	// 超过它的长度字段一定来自损坏帧而不是合法数据。
	maxFramePayload = 64 << 20 // 64 MiB
)

// ErrCorruptFrame 表示帧校验失败或被截断。
var ErrCorruptFrame = errors.New("corrupt spill frame")

type diskFrame struct {
	Seq      uint64
	Attempts uint32
	Dead     bool
	Payload  []byte
}

// encodeFrame 把一帧编码成自描述字节串。
func encodeFrame(f diskFrame) []byte {
	body := make([]byte, headerLen+len(f.Payload))
	binary.BigEndian.PutUint64(body[0:8], f.Seq)
	binary.BigEndian.PutUint32(body[8:12], f.Attempts)
	if f.Dead {
		body[12] = 1
	}
	binary.BigEndian.PutUint32(body[13:17], uint32(len(f.Payload)))
	copy(body[17:], f.Payload)

	out := make([]byte, 0, len(frameMagic)+4+len(body)+crcLen)
	out = append(out, frameMagic[:]...)
	var lbuf [4]byte
	binary.BigEndian.PutUint32(lbuf[:], uint32(len(body)))
	out = append(out, lbuf[:]...)
	out = append(out, body...)
	var cbuf [4]byte
	binary.BigEndian.PutUint32(cbuf[:], crc32.ChecksumIEEE(body))
	out = append(out, cbuf[:]...)
	return out
}

// marshalRecord 是落盘前的 JSON 编码。
func marshalRecord(record map[string]any) ([]byte, error) {
	return json.Marshal(record)
}

// unmarshalRecord 是回放时的解码。
func unmarshalRecord(raw []byte) (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// pushbackReader 在普通 reader 之上支持任意字节的回退，
// 回退的字节优先于底层数据被读出，供损坏帧重同步时压回误读字节。
type pushbackReader struct {
	r    io.Reader
	push []byte
}

func (p *pushbackReader) Read(b []byte) (int, error) {
	if len(p.push) > 0 {
		n := copy(b, p.push)
		p.push = p.push[n:]
		return n, nil
	}
	return p.r.Read(b)
}

func (p *pushbackReader) unread(b []byte) {
	// 新回退的数据应当先被读到：追加到 push 头部。
	head := make([]byte, 0, len(b)+len(p.push))
	head = append(head, b...)
	head = append(head, p.push...)
	p.push = head
}

// frameReader 顺序读帧，支持损坏后按 magic 重同步。
type frameReader struct {
	r *pushbackReader
}

func newFrameReader(r io.Reader) *frameReader {
	return &frameReader{r: &pushbackReader{r: r}}
}

// readFrame 读下一帧。
//
// resynced 为 true 表示中间跳过了损坏字节（调用方应计入 corrupt 指标）。
// 干净读到文件尾返回 io.EOF。
func (fr *frameReader) readFrame() (diskFrame, bool, error) {
	resynced := false
	for {
		f, err := fr.readOne()
		if err == nil {
			return f, resynced, nil
		}
		if !errors.Is(err, ErrCorruptFrame) {
			return diskFrame{}, resynced, err
		}
		// 损坏帧：扫描下一个 magic，扫描期间跳过的字节全部计为一次重同步。
		resynced = true
		if !fr.resync() {
			return diskFrame{}, true, io.EOF
		}
	}
}

// readOne 读取并校验单个帧，要求当前读取位置正对 magic。
func (fr *frameReader) readOne() (diskFrame, error) {
	var magic [4]byte
	if _, err := io.ReadFull(fr.r, magic[:]); err != nil {
		return diskFrame{}, err // 干净 EOF 或截断（截断由上层按 EOF 处理）
	}
	if magic != frameMagic {
		// 压回误读字节，交给 resync 逐字节扫描。
		fr.r.unread(magic[:])
		return diskFrame{}, fmt.Errorf("%w: bad magic %x", ErrCorruptFrame, magic)
	}

	var lbuf [4]byte
	if _, err := io.ReadFull(fr.r, lbuf[:]); err != nil {
		return diskFrame{}, fmt.Errorf("%w: truncated length: %v", ErrCorruptFrame, err)
	}
	bodyLen := binary.BigEndian.Uint32(lbuf[:])
	if bodyLen < headerLen || bodyLen > headerLen+maxFramePayload {
		return diskFrame{}, fmt.Errorf("%w: implausible body length %d", ErrCorruptFrame, bodyLen)
	}

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(fr.r, body); err != nil {
		return diskFrame{}, fmt.Errorf("%w: truncated body: %v", ErrCorruptFrame, err)
	}
	var cbuf [4]byte
	if _, err := io.ReadFull(fr.r, cbuf[:]); err != nil {
		return diskFrame{}, fmt.Errorf("%w: truncated crc: %v", ErrCorruptFrame, err)
	}
	if binary.BigEndian.Uint32(cbuf[:]) != crc32.ChecksumIEEE(body) {
		return diskFrame{}, fmt.Errorf("%w: checksum mismatch", ErrCorruptFrame)
	}

	payloadLen := binary.BigEndian.Uint32(body[13:17])
	if int(payloadLen) != len(body)-headerLen {
		return diskFrame{}, fmt.Errorf("%w: payload length mismatch", ErrCorruptFrame)
	}
	return diskFrame{
		Seq:      binary.BigEndian.Uint64(body[0:8]),
		Attempts: binary.BigEndian.Uint32(body[8:12]),
		Dead:     body[12] == 1,
		Payload:  append([]byte(nil), body[17:]...),
	}, nil
}

// resync 逐字节扫描直到再次遇到完整 magic，读取位置最终正对 magic 开头。
// 误判时把多读的字节压回，保证不漏掉嵌在其中的候选。返回 false 表示到文件尾。
func (fr *frameReader) resync() bool {
	for {
		var b [1]byte
		if _, err := io.ReadFull(fr.r, b[:]); err != nil {
			return false
		}
		if b[0] != frameMagic[0] {
			continue
		}
		var rest [3]byte
		n, _ := io.ReadFull(fr.r, rest[:])
		if n < 3 {
			return false
		}
		if rest == [3]byte{frameMagic[1], frameMagic[2], frameMagic[3]} {
			// 完整命中：把 4 字节 magic 整体压回，下一轮 readOne 从 magic 读起。
			fr.r.unread(append([]byte{frameMagic[0]}, rest[:]...))
			return true
		}
		// 误命中：压回后续字节继续扫描（首字节已确认不是新候选的一部分场景
		// 仍可能在 rest 中出现 magic[0]，压回后逐字节扫描不会遗漏）。
		fr.r.unread(rest[:n])
	}
}
