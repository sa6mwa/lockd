#!/bin/bash

for T in mem disk aws azure minio ; do
	set -a && source .env.${T} && set +a && go test -timeout 2m -v -tags "integration ${T}" -count=1 ./integration/... 2>&1 | tee ${T}fail.log
	set -a && source .env.${T} && set +a && go test -timeout 2m -v -tags "integration ${T} lq" -count=1 ./integration/... 2>&1 | tee ${T}lqfail.log
done

echo
echo "Failed:"

for T in mem disk aws azure minio ; do
	rg FAIL ${T}fail.log 2>&1 > /dev/null && echo ${T}fail.log
	rg FAIL ${T}lqfail.log 2>&1 > /dev/null && echo ${T}lqfail.log
done

echo

for T in disk aws azure minio ; do
	echo "set -a && source .env.${T} && set +a && go test -timeout 2m -v -tags \"integration ${T}\" -count=1 ./integration/... 2>&1 | tee ${T}fail.log"
done
