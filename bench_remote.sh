#!/bin/bash -xe
cd $(cd $(dirname $0);pwd)
../sync_remote.sh
ssh -t vh403 -L 2233:localhost:2233 "cd release/simple_kvs; python3 -m run"
