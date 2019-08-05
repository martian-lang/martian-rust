#!/bin/bash
MROPATH=$PWD
MROFLAGS="--disable-ui"
PATH=$(realpath ../../../target/release/examples/):$PATH
mrp pipeline.mro pipeline_test --jobmode=local
