package transformer

import (
	"strings"
	"testing"

	sql "github.com/SisyphusSQ/godropbox/database/sqlbuilder"

	"github.com/SisyphusSQ/my2sql/internal/models"
)

func TestValuesEqual_BinaryAndScalarColumns(t *testing.T) {
	tr := newTestTransformer()

	if !tr.valuesEqual(1, []byte("same"), []byte("same")) {
		t.Fatalf("expected binary values to be equal")
	}
	if tr.valuesEqual(1, []byte("left"), []byte("right")) {
		t.Fatalf("expected binary values to be different")
	}
	if !tr.valuesEqual(0, 1, 1) {
		t.Fatalf("expected scalar values to be equal")
	}
}

func TestGenUpdSetPart_OnlyWritesChangedColumns(t *testing.T) {
	tr := newTestTransformer()

	update := sql.NewTable("orders", tr.curColsDef...).Update()
	update = tr.genUpdSetPart(update, []any{1, []byte("same"), "after"}, []any{1, []byte("same"), "before"})
	update.Where(sql.EqL(tr.curColsDef[0], 1))

	sqlText, err := update.String("shop")
	if err != nil {
		t.Fatalf("unexpected error generating update sql: %v", err)
	}
	setClause := strings.Split(strings.Split(sqlText, " SET ")[1], " WHERE ")[0]

	if !strings.Contains(setClause, "name") {
		t.Fatalf("expected changed column to be present in SET clause, got %s", sqlText)
	}
	if strings.Contains(setClause, "payload") {
		t.Fatalf("expected unchanged binary column to be omitted from SET clause, got %s", sqlText)
	}
	if strings.Contains(setClause, "id") {
		t.Fatalf("expected unchanged scalar column to be omitted from SET clause, got %s", sqlText)
	}
}

func newTestTransformer() *Transformer {
	return &Transformer{
		curTbInfo: &models.TblInfo{
			Columns: []*models.FieldInfo{
				{Index: 0, FieldName: "id", FieldType: "int"},
				{Index: 1, FieldName: "payload", FieldType: "blob"},
				{Index: 2, FieldName: "name", FieldType: "varchar"},
			},
		},
		curColsDef: []sql.NonAliasColumn{
			sql.IntColumn("id", sql.NotNullable),
			sql.BytesColumn("payload", sql.NotNullable),
			sql.StrColumn("name", sql.UTF8, sql.UTF8CaseInsensitive, sql.NotNullable),
		},
	}
}
