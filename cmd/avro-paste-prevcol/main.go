package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	p2 "github.com/takanoriyanagitani/go-avro-paste-prevcol"
	. "github.com/takanoriyanagitani/go-avro-paste-prevcol/util"

	pr "github.com/takanoriyanagitani/go-avro-paste-prevcol/pastedrow"

	dh "github.com/takanoriyanagitani/go-avro-paste-prevcol/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-paste-prevcol/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var originals IO[iter.Seq2[p2.OriginalRow, error]] = Bind(
	stdin2maps,
	Lift(func(
		m iter.Seq2[map[string]any, error],
	) (iter.Seq2[p2.OriginalRow, error], error) {
		return func(yield func(p2.OriginalRow, error) bool) {
			for row, e := range m {
				if !yield(row, e) {
					return
				}
			}
		}, nil
	}),
)

var targetColumn IO[string] = EnvValByKey("ENV_TARGET_COLUMN_NAME")
var pastedColumn IO[string] = EnvValByKey("ENV_PASTED_COLUMN_NAME")

var config IO[pr.Config] = Bind(
	All(
		targetColumn,
		pastedColumn,
	),
	Lift(func(s []string) (pr.Config, error) {
		return pr.Config{
			TargetColumnName: p2.TargetColumnName(s[0]),
			PastedColumnName: p2.PastedColumnName(s[1]),
			NullableToAny:    true,
		}, nil
	}),
)

var pasted IO[iter.Seq2[p2.PastedRow, error]] = Bind(
	config,
	func(c pr.Config) IO[iter.Seq2[p2.PastedRow, error]] {
		return Bind(
			originals,
			c.RowsToPastedRows,
		)
	},
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	pasted,
	Lift(func(
		p iter.Seq2[p2.PastedRow, error],
	) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			for row, e := range p {
				if !yield(row, e) {
					return
				}
			}
		}, nil
	}),
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
