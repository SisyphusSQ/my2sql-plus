package loader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/bits"
	"os"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/SisyphusSQ/my2sql/internal/config"
	"github.com/SisyphusSQ/my2sql/internal/log"
	"github.com/SisyphusSQ/my2sql/internal/models"
	"github.com/SisyphusSQ/my2sql/internal/vars"
)

var flashbackCompressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

// FlashbackLoader 基于原始 row event 生成反向 binlog。
type FlashbackLoader struct {
	wg  *sync.WaitGroup
	ctx context.Context

	outputFile string
	file       *os.File

	currentPos      uint32
	checksumEnabled bool
	formatDescRaw   []byte
	pendingRows     []*models.FlashbackEvent

	flashbackChan <-chan *models.FlashbackEvent
}

func NewFlashbackLoader(wg *sync.WaitGroup, ctx context.Context,
	c *config.Config, flashbackChan chan *models.FlashbackEvent) *FlashbackLoader {
	return &FlashbackLoader{
		wg:            wg,
		ctx:           ctx,
		outputFile:    c.FlashbackBinlogPath + vars.FlashbackBinlogSuffix,
		flashbackChan: flashbackChan,
	}
}

func (f *FlashbackLoader) Start() error {
	f.wg.Add(1)
	log.Logger.Info("start thread to write flashback binlog into file [file=%s]", f.outputFile)

	for {
		select {
		case <-f.ctx.Done():
			return nil
		case ev, ok := <-f.flashbackChan:
			if !ok {
				if len(f.pendingRows) > 0 {
					log.Logger.Warn("skip incomplete transaction for flashback binlog, rows=%d", len(f.pendingRows))
				}
				return nil
			}

			if err := f.handleEvent(ev); err != nil {
				return err
			}
		}
	}
}

func (f *FlashbackLoader) handleEvent(ev *models.FlashbackEvent) error {
	switch ev.EventType {
	case replication.FORMAT_DESCRIPTION_EVENT:
		if len(f.formatDescRaw) == 0 {
			f.formatDescRaw = bytes.Clone(ev.RawData)
			if ev.FormatDescription != nil {
				f.checksumEnabled = ev.FormatDescription.ChecksumAlgorithm == replication.BINLOG_CHECKSUM_ALG_CRC32
			}
		}
	case replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:
		f.pendingRows = append(f.pendingRows, ev)
	case replication.XID_EVENT:
		if len(f.pendingRows) == 0 {
			return nil
		}
		return f.flushTransaction(ev)
	default:
		// do nothing
	}

	return nil
}

func (f *FlashbackLoader) flushTransaction(commitEvent *models.FlashbackEvent) error {
	if err := f.ensureOutputFile(); err != nil {
		return err
	}

	for idx := len(f.pendingRows) - 1; idx >= 0; idx-- {
		ev := f.pendingRows[idx]
		if len(ev.TableMapRawData) == 0 {
			return fmt.Errorf("missing table map raw data for flashback row event")
		}
		if err := f.writeRawEvent(ev.TableMapRawData, replication.TABLE_MAP_EVENT, nil); err != nil {
			return err
		}

		bodyRewrite := func(body []byte) ([]byte, error) {
			return rollbackRowsBody(ev, body)
		}
		if err := f.writeRawEvent(ev.RawData, rollbackEventType(ev.EventType), bodyRewrite); err != nil {
			return err
		}
	}

	commitRaw := commitEvent.RawData
	if len(commitRaw) == 0 {
		commitRaw = buildSyntheticXIDEvent(lastTimestamp(f.pendingRows), f.checksumEnabled)
	}
	if err := f.writeRawEvent(commitRaw, replication.XID_EVENT, nil); err != nil {
		return err
	}

	f.pendingRows = nil
	return nil
}

func (f *FlashbackLoader) ensureOutputFile() error {
	if f.file != nil {
		return nil
	}
	if len(f.formatDescRaw) == 0 {
		return fmt.Errorf("missing format description event for flashback binlog output")
	}

	file, err := os.OpenFile(f.outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if _, err = file.Write(replication.BinLogFileHeader); err != nil {
		_ = file.Close()
		return err
	}

	f.file = file
	f.currentPos = uint32(len(replication.BinLogFileHeader))

	return f.writeRawEvent(f.formatDescRaw, replication.FORMAT_DESCRIPTION_EVENT, nil)
}

func (f *FlashbackLoader) writeRawEvent(raw []byte, eventType replication.EventType,
	bodyRewrite func([]byte) ([]byte, error)) error {
	event := bytes.Clone(raw)
	if len(event) < replication.EventHeaderSize {
		return fmt.Errorf("invalid raw event size: %d", len(event))
	}

	event[4] = byte(eventType)

	checksumLen := 0
	if f.checksumEnabled {
		checksumLen = replication.BinlogChecksumLength
	}

	if bodyRewrite != nil {
		bodyEnd := len(event) - checksumLen
		body, err := bodyRewrite(event[replication.EventHeaderSize:bodyEnd])
		if err != nil {
			return err
		}
		if len(body) != bodyEnd-replication.EventHeaderSize {
			return fmt.Errorf("unexpected flashback body size %d", len(body))
		}
		copy(event[replication.EventHeaderSize:bodyEnd], body)
	}

	nextPos := f.currentPos + uint32(len(event))
	binary.LittleEndian.PutUint32(event[13:17], nextPos)

	if f.checksumEnabled {
		checksum := crc32.ChecksumIEEE(event[:len(event)-replication.BinlogChecksumLength])
		binary.LittleEndian.PutUint32(event[len(event)-replication.BinlogChecksumLength:], checksum)
	}

	if _, err := f.file.Write(event); err != nil {
		return err
	}
	f.currentPos = nextPos
	return nil
}

func (f *FlashbackLoader) LastBinlog() string {
	return f.outputFile
}

func (f *FlashbackLoader) Stop() {
	if f.file != nil {
		_ = f.file.Close()
	}

	f.wg.Done()
	log.Logger.Info("exit thread to write flashback binlog into file [file=%s]", f.outputFile)
}

func rollbackEventType(eventType replication.EventType) replication.EventType {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv1:
		return replication.DELETE_ROWS_EVENTv1
	case replication.WRITE_ROWS_EVENTv2:
		return replication.DELETE_ROWS_EVENTv2
	case replication.DELETE_ROWS_EVENTv1:
		return replication.WRITE_ROWS_EVENTv1
	case replication.DELETE_ROWS_EVENTv2:
		return replication.WRITE_ROWS_EVENTv2
	default:
		return eventType
	}
}

func rollbackRowsBody(ev *models.FlashbackEvent, body []byte) ([]byte, error) {
	switch ev.EventType {
	case replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:
		return bytes.Clone(body), nil
	case replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		return reverseUpdateRowsBody(ev, body)
	default:
		return nil, fmt.Errorf("unsupported flashback row event type %s", ev.EventType)
	}
}

func reverseUpdateRowsBody(ev *models.FlashbackEvent, body []byte) ([]byte, error) {
	if ev.RowsEvent == nil || ev.RowsEvent.Table == nil {
		return nil, fmt.Errorf("missing rows event metadata for update flashback")
	}

	headerLen, err := rowsEventHeaderLength(ev.EventType, ev.RowsEvent, body)
	if err != nil {
		return nil, err
	}

	rowPayload := body[headerLen:]
	reversedPayload := make([]byte, 0, len(rowPayload))
	for pos := 0; pos < len(rowPayload); {
		beforeLen, err := rowImageLength(ev.RowsEvent.Table, ev.RowsEvent.ColumnBitmap1, rowPayload[pos:])
		if err != nil {
			return nil, err
		}
		afterStart := pos + beforeLen
		afterLen, err := rowImageLength(ev.RowsEvent.Table, ev.RowsEvent.ColumnBitmap2, rowPayload[afterStart:])
		if err != nil {
			return nil, err
		}

		reversedPayload = append(reversedPayload, rowPayload[afterStart:afterStart+afterLen]...)
		reversedPayload = append(reversedPayload, rowPayload[pos:afterStart]...)
		pos = afterStart + afterLen
	}

	reversedBody := make([]byte, 0, len(body))
	reversedBody = append(reversedBody, body[:headerLen]...)
	reversedBody = append(reversedBody, reversedPayload...)
	return reversedBody, nil
}

func rowsEventHeaderLength(eventType replication.EventType, rowEvent *replication.RowsEvent, body []byte) (int, error) {
	pos := 8 // 6 bytes table id + 2 bytes flags

	switch eventType {
	case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
		if len(body) < pos+2 {
			return 0, fmt.Errorf("invalid rows event body size: %d", len(body))
		}
		extraDataLen := int(binary.LittleEndian.Uint16(body[pos : pos+2]))
		pos += extraDataLen
	}

	_, _, n := mysql.LengthEncodedInt(body[pos:])
	pos += n

	bitmapLen := bitmapByteSize(int(rowEvent.ColumnCount))
	pos += bitmapLen
	if isUpdateRowsEvent(eventType) {
		pos += bitmapLen
	}
	if pos > len(body) {
		return 0, fmt.Errorf("invalid rows header length %d, body=%d", pos, len(body))
	}
	return pos, nil
}

func rowImageLength(table *replication.TableMapEvent, bitmap []byte, data []byte) (int, error) {
	nullBitmapLen := bitmapByteSize(bitmapBitCount(bitmap, int(table.ColumnCount)))
	if len(data) < nullBitmapLen {
		return 0, fmt.Errorf("invalid row image size: %d", len(data))
	}

	pos := nullBitmapLen
	nullBitmap := data[:nullBitmapLen]
	nullBitmapIndex := 0

	for colIdx := 0; colIdx < int(table.ColumnCount); colIdx++ {
		if !bitmapBitSet(bitmap, colIdx) {
			continue
		}

		if bitmapBitSet(nullBitmap, nullBitmapIndex) {
			nullBitmapIndex++
			continue
		}
		nullBitmapIndex++

		valueLen, err := binlogValueLength(data[pos:], table.ColumnType[colIdx], table.ColumnMeta[colIdx])
		if err != nil {
			return 0, err
		}
		pos += valueLen
		if pos > len(data) {
			return 0, fmt.Errorf("row image exceeds payload size")
		}
	}

	return pos, nil
}

func binlogValueLength(data []byte, tp byte, meta uint16) (int, error) {
	length := 0
	if tp == mysql.MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = b0 | 0x30
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}

	switch tp {
	case mysql.MYSQL_TYPE_NULL:
		return 0, nil
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT:
		return 4, nil
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_YEAR:
		return 1, nil
	case mysql.MYSQL_TYPE_SHORT:
		return 2, nil
	case mysql.MYSQL_TYPE_INT24:
		return 3, nil
	case mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_DOUBLE, mysql.MYSQL_TYPE_DATETIME:
		return 8, nil
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		precision := int(uint8(meta >> 8))
		decimals := int(uint8(meta & 0xFF))
		integral := precision - decimals
		uncompIntegral := integral / 9
		uncompFractional := decimals / 9
		compIntegral := integral - uncompIntegral*9
		compFractional := decimals - uncompFractional*9

		return uncompIntegral*4 + flashbackCompressedBytes[compIntegral] +
			uncompFractional*4 + flashbackCompressedBytes[compFractional], nil
	case mysql.MYSQL_TYPE_BIT:
		nbits := ((meta >> 8) * 8) + (meta & 0xFF)
		return int(nbits+7) / 8, nil
	case mysql.MYSQL_TYPE_TIMESTAMP:
		return 4, nil
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		return 4 + int((meta+1)/2), nil
	case mysql.MYSQL_TYPE_DATETIME2:
		return 5 + int((meta+1)/2), nil
	case mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_DATE:
		return 3, nil
	case mysql.MYSQL_TYPE_TIME2:
		return 3 + int((meta+1)/2), nil
	case mysql.MYSQL_TYPE_ENUM:
		switch meta & 0xFF {
		case 1:
			return 1, nil
		case 2:
			return 2, nil
		default:
			return 0, fmt.Errorf("unknown enum pack length %d", meta&0xFF)
		}
	case mysql.MYSQL_TYPE_SET:
		return int(meta & 0xFF), nil
	case mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_JSON, mysql.MYSQL_TYPE_GEOMETRY:
		prefixLen := int(meta)
		if len(data) < prefixLen {
			return 0, fmt.Errorf("insufficient bytes for blob-like value")
		}
		return prefixLen + int(fixedLengthInt(data[:prefixLen])), nil
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		if meta < 256 {
			return 1 + int(data[0]), nil
		}
		return 2 + int(binary.LittleEndian.Uint16(data[:2])), nil
	case mysql.MYSQL_TYPE_STRING:
		if length < 256 {
			return 1 + int(data[0]), nil
		}
		return 2 + int(binary.LittleEndian.Uint16(data[:2])), nil
	default:
		return 0, fmt.Errorf("unsupported mysql type %d in flashback binlog", tp)
	}
}

func buildSyntheticXIDEvent(timestamp uint32, checksumEnabled bool) []byte {
	size := replication.EventHeaderSize + 8
	if checksumEnabled {
		size += replication.BinlogChecksumLength
	}

	raw := make([]byte, size)
	binary.LittleEndian.PutUint32(raw[0:4], timestamp)
	raw[4] = byte(replication.XID_EVENT)
	binary.LittleEndian.PutUint32(raw[5:9], 1)
	binary.LittleEndian.PutUint32(raw[9:13], uint32(size))
	binary.LittleEndian.PutUint16(raw[17:19], 0)
	binary.LittleEndian.PutUint64(raw[19:27], 1)
	return raw
}

func lastTimestamp(rows []*models.FlashbackEvent) uint32 {
	if len(rows) == 0 {
		return 0
	}
	return rows[len(rows)-1].Timestamp
}

func isUpdateRowsEvent(eventType replication.EventType) bool {
	switch eventType {
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return true
	default:
		return false
	}
}

func bitmapByteSize(bitCount int) int {
	return (bitCount + 7) / 8
}

func bitmapBitCount(bitmap []byte, maxBits int) int {
	count := 0
	for _, b := range bitmap {
		count += bits.OnesCount8(b)
	}
	if count > maxBits {
		return maxBits
	}
	return count
}

func bitmapBitSet(bitmap []byte, idx int) bool {
	return bitmap[idx>>3]&(1<<(uint(idx)&7)) > 0
}

func fixedLengthInt(data []byte) uint64 {
	var value uint64
	for idx, b := range data {
		value |= uint64(b) << (8 * idx)
	}
	return value
}
