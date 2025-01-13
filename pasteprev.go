package pasteprev

import (
	"database/sql"
)

type OriginalRow map[string]any
type PastedRow map[string]any

type PreviousColumn sql.Null[any]
type PreviousRow sql.Null[map[string]any]

func (p PreviousColumn) ToAny() any {
	switch p.Valid {
	case true:
		return p.V
	default:
		return nil
	}
}

type TargetColumnName string
type PastedColumnName string

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct{ BlobSizeMax int }

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
