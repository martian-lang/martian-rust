#!/bin/bash
MROPATH=$PWD
MROFLAGS="--disable-ui"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PATH=$(realpath ${SCRIPT_DIR}/../../../deps/bin/):$PATH
PATH=$(realpath ${SCRIPT_DIR}/../../../target/debug/examples/):$PATH
mrp pipeline.mro pipeline_test --jobmode=local --disable-ui
