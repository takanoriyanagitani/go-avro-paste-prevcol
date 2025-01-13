#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/sample.avro
}

#genavro

export ENV_TARGET_COLUMN_NAME=height
export ENV_PASTED_COLUMN_NAME=height_previous
export ENV_SCHEMA_FILENAME=./sample.d/output.avsc

cat ./sample.d/sample.avro |
	./avro-paste-prevcol |
	rq -aJ |
	jq -c
