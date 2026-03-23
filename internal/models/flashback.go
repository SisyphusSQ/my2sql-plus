package models

import "github.com/go-mysql-org/go-mysql/replication"

// FlashbackEvent 反向 binlog 输出所需的最小事件载荷。
type FlashbackEvent struct {
	EventType replication.EventType
	TrxIndex  uint64
	Timestamp uint32
	SQLType   string

	RawData         []byte
	TableMapRawData []byte

	RowsEvent         *replication.RowsEvent
	FormatDescription *replication.FormatDescriptionEvent
}
