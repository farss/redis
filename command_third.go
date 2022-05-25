package redis

import (
	"context"
	"fmt"
	"github.com/farss/redis/v8/internal/proto"
	"github.com/farss/redis/v8/internal/util"
	"sync"
)

const (
	ScanDefault  = 0
	ScanIncluded = 1
	ScanValue    = 2
)

type KvScanCmd struct {
	baseCmd

	page   []string
	cursor string

	process cmdable
}

var _ Cmder = (*KvScanCmd)(nil)

func NewKvScanCmd(ctx context.Context, process cmdable, args ...interface{}) *KvScanCmd {
	return &KvScanCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
		process: process,
	}
}

func (cmd *KvScanCmd) SetVal(page []string, cursor string) {
	cmd.page = page
	cmd.cursor = cursor
}

func (cmd *KvScanCmd) Val() (keys []string, cursor string) {
	return cmd.page, cmd.cursor
}

func (cmd *KvScanCmd) Result() (keys []string, cursor string, err error) {
	return cmd.page, cmd.cursor, cmd.err
}

type KeyValue struct {
	Key   string
	Value []byte
}

func (cmd *KvScanCmd) KeyVal() (kvs []*KeyValue, cursor string, err error) {
	for i := 0; i < len(cmd.page); i += 2 {
		kvs = append(kvs, &KeyValue{Key: cmd.page[i], Value: util.StringToBytes(cmd.page[i+1])})
	}
	return kvs, cmd.cursor, cmd.err
}

func (cmd *KvScanCmd) String() string {
	return cmdString(cmd, cmd.page)
}

func (cmd *KvScanCmd) readReply(rd *proto.Reader) (err error) {
	cmd.page, cmd.cursor, err = cmd.readScanReply(rd)
	return err
}

func (cmd *KvScanCmd) readScanReply(r *proto.Reader) ([]string, string, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, "", err
	}
	if n != 2 {
		return nil, "", fmt.Errorf("redis: got %d elements in scan reply, expected 2", n)
	}

	cursor, err := r.ReadString()
	if err != nil {
		return nil, "", err
	}

	n, err = r.ReadArrayLen()
	if err != nil {
		return nil, "", err
	}

	keys := make([]string, n)

	for i := 0; i < n; i++ {
		key, err := r.ReadString()
		if err != nil && err != Nil {
			return nil, "", err
		}
		keys[i] = key
	}

	return keys, cursor, err
}

// Iterator creates a new ScanIterator.
func (cmd *KvScanCmd) Iterator() *KvScanIterator {
	return &KvScanIterator{
		cmd: cmd,
	}
}

// KvScanIterator is used to incrementally iterate over a collection of elements.
// It's safe for concurrent use by multiple goroutines.
type KvScanIterator struct {
	mu  sync.Mutex // protects Scanner and pos
	cmd *KvScanCmd
	pos int
}

// Err returns the last iterator error, if any.
func (it *KvScanIterator) Err() error {
	it.mu.Lock()
	err := it.cmd.Err()
	it.mu.Unlock()
	return err
}

// Next advances the cursor and returns true if more values can be read.
func (it *KvScanIterator) Next(ctx context.Context) bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Instantly return on errors.
	if it.cmd.Err() != nil {
		return false
	}

	// Advance cursor, check if we are still within range.
	if it.pos < len(it.cmd.page) {
		it.pos++
		return true
	}

	for {
		// Return if there is no more data to fetch.
		if it.cmd.cursor == "0" {
			return false
		}

		// Fetch next page.
		switch it.cmd.args[0] {
		case "scan", "qscan":
			it.cmd.args[1] = it.cmd.cursor
		default:
			it.cmd.args[2] = it.cmd.cursor
		}

		err := it.cmd.process(ctx, it.cmd)
		if err != nil {
			return false
		}

		it.pos = 1

		// Redis can occasionally return empty page.
		if len(it.cmd.page) > 0 {
			return true
		}
	}
}

// Val returns the key/field at the current cursor position.
func (it *KvScanIterator) Val() string {
	var v string
	it.mu.Lock()
	if it.cmd.Err() == nil && it.pos > 0 && it.pos <= len(it.cmd.page) {
		v = it.cmd.page[it.pos-1]
	}
	it.mu.Unlock()
	return v
}

func (it *KvScanIterator) KeyVal() (k string, v string) {
	it.mu.Lock()
	if it.cmd.Err() == nil && it.pos > 0 && it.pos <= len(it.cmd.page) {
		k = it.cmd.page[it.pos-1]
		it.pos++
		if it.pos <= len(it.cmd.page) {
			v = it.cmd.page[it.pos-1]
		}
	}
	it.mu.Unlock()
	return k, v
}

func (it *KvScanIterator) KeyValBytes() (k string, v []byte) {
	it.mu.Lock()
	if it.cmd.Err() == nil && it.pos > 0 && it.pos <= len(it.cmd.page) {
		k = it.cmd.page[it.pos-1]
		it.pos++
		if it.pos <= len(it.cmd.page) {
			v = util.StringToBytes(it.cmd.page[it.pos-1])
		}
	}
	it.mu.Unlock()
	return k, v
}
