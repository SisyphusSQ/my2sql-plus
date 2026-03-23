package loader

import (
	"bytes"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/SisyphusSQ/my2sql/internal/models"
)

func TestReverseUpdateRowsBody_SwapsBeforeAndAfterImages(t *testing.T) {
	rowEvent := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			ColumnCount: 2,
			ColumnType:  []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR},
			ColumnMeta:  []uint16{0, 20},
		},
		ColumnCount:   2,
		ColumnBitmap1: []byte{0x03},
		ColumnBitmap2: []byte{0x03},
	}

	header := []byte{
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // table id
		0x00, 0x00, // flags
		0x02, // column count
		0x03, // before bitmap
		0x03, // after bitmap
	}
	beforeImage := []byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 'a'}
	afterImage := []byte{0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 'b'}
	body := append(bytes.Clone(header), append(beforeImage, afterImage...)...)

	reversed, err := reverseUpdateRowsBody(&models.FlashbackEvent{
		EventType: replication.UPDATE_ROWS_EVENTv1,
		RowsEvent: rowEvent,
	}, body)
	if err != nil {
		t.Fatalf("unexpected reverse update error: %v", err)
	}

	expected := append(bytes.Clone(header), append(afterImage, beforeImage...)...)
	if !bytes.Equal(reversed, expected) {
		t.Fatalf("unexpected reversed body:\nexpected=%v\ngot=%v", expected, reversed)
	}
}

func TestRollbackEventType(t *testing.T) {
	tests := []struct {
		name     string
		input    replication.EventType
		expected replication.EventType
	}{
		{name: "write v1", input: replication.WRITE_ROWS_EVENTv1, expected: replication.DELETE_ROWS_EVENTv1},
		{name: "write v2", input: replication.WRITE_ROWS_EVENTv2, expected: replication.DELETE_ROWS_EVENTv2},
		{name: "delete v1", input: replication.DELETE_ROWS_EVENTv1, expected: replication.WRITE_ROWS_EVENTv1},
		{name: "delete v2", input: replication.DELETE_ROWS_EVENTv2, expected: replication.WRITE_ROWS_EVENTv2},
		{name: "update unchanged", input: replication.UPDATE_ROWS_EVENTv2, expected: replication.UPDATE_ROWS_EVENTv2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rollbackEventType(tt.input); got != tt.expected {
				t.Fatalf("unexpected rollback event type: got=%s want=%s", got, tt.expected)
			}
		})
	}
}
