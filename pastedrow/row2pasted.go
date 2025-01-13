package pastedrow

import (
	"context"
	"database/sql"
	"iter"

	p2 "github.com/takanoriyanagitani/go-avro-paste-prevcol"
	. "github.com/takanoriyanagitani/go-avro-paste-prevcol/util"
)

type Previous struct {
	p2.PreviousRow
	p2.TargetColumnName
}

func (p Previous) MapToPreviousColumn(m map[string]any) p2.PreviousColumn {
	var key string = string(p.TargetColumnName)
	val, found := m[key]

	var ret p2.PreviousColumn
	ret.Valid = found
	ret.V = val
	return ret
}

func (p Previous) ToPreviousColumn() p2.PreviousColumn {
	switch p.PreviousRow.Valid {
	case false:
		return p2.PreviousColumn{}
	default:
		return p.MapToPreviousColumn(p.PreviousRow.V)
	}
}

type Config struct {
	p2.TargetColumnName
	p2.PastedColumnName
	NullableToAny bool
}

func (c Config) SetPrevious() func(map[string]any, string, sql.Null[any]) {
	switch c.NullableToAny {
	case false:
		return func(m map[string]any, colname string, n sql.Null[any]) {
			m[colname] = n
		}
	default:
		return func(m map[string]any, colname string, n sql.Null[any]) {
			prev := p2.PreviousColumn(n)
			m[colname] = prev.ToAny()
		}
	}
}

func (c Config) RowsToPastedRows(
	originals iter.Seq2[p2.OriginalRow, error],
) IO[iter.Seq2[p2.PastedRow, error]] {
	buf := p2.PastedRow{}
	prev := p2.PreviousColumn{}
	return func(_ context.Context) (iter.Seq2[p2.PastedRow, error], error) {
		return func(yield func(p2.PastedRow, error) bool) {
			var setPrevious func(map[string]any, string, sql.Null[any]) = c.
				SetPrevious()
			for row, e := range originals {
				clear(buf)

				if nil != e {
					yield(nil, e)
					return
				}

				for key, val := range row {
					buf[key] = val

					if key != string(c.TargetColumnName) {
						continue
					}

					var pkey string = string(c.PastedColumnName)
					switch prev.Valid {
					case true:
						setPrevious(buf, pkey, sql.Null[any]{
							Valid: true,
							V:     prev.V,
						})
					default:
						setPrevious(buf, pkey, sql.Null[any]{})
					}

					prev.Valid = true
					prev.V = val
				}

				if !yield(buf, nil) {
					return
				}
			}
		}, nil
	}
}
