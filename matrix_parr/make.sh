#!/bin/bash

# 获取当前脚本所在的目录
script_dir="$(dirname "$0")"

# 进入源代码目录
cd "$script_dir/../src"
make clean
make -j

# 返回到脚本所在目录
cd "$script_dir/../matrix_parr"

# 删除日志文件
rm -rf log.*
make clean
make matrix_parr