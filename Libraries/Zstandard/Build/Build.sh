#!/bin/bash
cd zstd/lib
make ZSTD_LEGACY_SUPPORT=0 ZSTD_LIB_DEPRECATED=0 ZSTD_NO_UNUSED_FUNCTIONS=1
mv libzstd.a ../../libzstdpack.a