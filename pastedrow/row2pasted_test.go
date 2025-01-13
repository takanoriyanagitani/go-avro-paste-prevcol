package pastedrow_test

import (
	"testing"

	"context"
	"database/sql"
	"iter"

	p2 "github.com/takanoriyanagitani/go-avro-paste-prevcol"
	. "github.com/takanoriyanagitani/go-avro-paste-prevcol/util"

	pr "github.com/takanoriyanagitani/go-avro-paste-prevcol/pastedrow"
)

func TestRowToPasted(t *testing.T) {
	t.Parallel()

	var ctx context.Context = context.Background()

	t.Run("Config", func(t *testing.T) {
		t.Parallel()

		cfg := pr.Config{
			TargetColumnName: "height",
			PastedColumnName: "height_previous",
			NullableToAny:    false,
		}

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var input iter.Seq2[p2.OriginalRow, error] = func(
				yield func(p2.OriginalRow, error) bool,
			) {
			}

			var pasted IO[iter.Seq2[p2.PastedRow, error]] = cfg.
				RowsToPastedRows(input)

			var i iter.Seq2[p2.PastedRow, error] = pasted.Must(ctx)
			next, stop := iter.Pull2(i)
			defer stop()
			_, _, found := next()
			if found {
				t.Fatal("must be empty")
			}
		})

		t.Run("single", func(t *testing.T) {
			t.Parallel()

			var input iter.Seq2[p2.OriginalRow, error] = func(
				yield func(p2.OriginalRow, error) bool,
			) {
				yield(p2.OriginalRow{
					"height": 3.776,
				}, nil)
			}

			var pasted IO[iter.Seq2[p2.PastedRow, error]] = cfg.
				RowsToPastedRows(input)

			var i iter.Seq2[p2.PastedRow, error] = pasted.Must(ctx)
			next, stop := iter.Pull2(i)
			defer stop()

			row, e, found := next()
			if !found {
				t.Fatal("must be non empty")
			}

			if nil != e {
				t.Fatalf("unexpected error: %v\n", e)
			}

			var height float64 = row["height"].(float64)
			if 3.776 != height {
				t.Fatalf("unexpected value: %v\n", height)
			}

			var previousHeight sql.Null[any] = row["height_previous"].(sql.Null[any])
			if previousHeight.Valid {
				t.Fatalf("must be empty")
			}

			_, _, found = next()
			if found {
				t.Fatal("must be empty")
			}
		})

		t.Run("multi", func(t *testing.T) {
			t.Parallel()

			var input iter.Seq2[p2.OriginalRow, error] = func(
				yield func(p2.OriginalRow, error) bool,
			) {
				if !yield(p2.OriginalRow{
					"height": 3.776,
					"name":   "fuji",
				}, nil) {
					return
				}

				yield(p2.OriginalRow{
					"height": 0.599,
					"name":   "takao",
				}, nil)
			}

			var pasted IO[iter.Seq2[p2.PastedRow, error]] = cfg.
				RowsToPastedRows(input)

			var i iter.Seq2[p2.PastedRow, error] = pasted.Must(ctx)
			next, stop := iter.Pull2(i)
			defer stop()

			row, e, found := next()
			if !found {
				t.Fatal("must be non empty")
			}

			if nil != e {
				t.Fatalf("unexpected error: %v\n", e)
			}

			var height float64 = row["height"].(float64)
			if 3.776 != height {
				t.Fatalf("unexpected value: %v\n", height)
			}

			var previousHeight sql.Null[any] = row["height_previous"].(sql.Null[any])
			if previousHeight.Valid {
				t.Fatalf("must be empty")
			}

			row, e, found = next()

			if !found {
				t.Fatal("must be non empty")
			}

			if nil != e {
				t.Fatalf("unexpected error: %v\n", e)
			}

			height = row["height"].(float64)
			if 0.599 != height {
				t.Fatalf("unexpected value: %v\n", height)
			}

			previousHeight = row["height_previous"].(sql.Null[any])
			if !previousHeight.Valid {
				t.Fatalf("must not be empty")
			}

			var prev float64 = previousHeight.V.(float64)
			if prev != 3.776 {
				t.Fatalf("unexpected value: %v\n", height)
			}
		})
	})
}
