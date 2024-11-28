package vars

import "errors"

var (
	SchemaTableEmpty = errors.New("schema/table is empty")
	ManualKill       = errors.New("finished by ctrl-C/kill/kill -15")
	NotSupportType   = errors.New("not support type")
)
