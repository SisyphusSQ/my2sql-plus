package vars

import "errors"

var (
	SchemaTableEmpty = errors.New("schema/table is empty")
	NotSupportType   = errors.New("not support type")
)
