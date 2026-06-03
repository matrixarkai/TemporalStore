// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#define DefaultDRAM 0x0
#define MAX_BLOCK_SIZE (1ULL << 32)
#define TABLE_SIZE (1 * 512 * 1024 * 1024ULL)
#define BUCKET_SIZE 512
#define ENTRY_SIZE 32
#define BUCKET_NUM (TABLE_SIZE / BUCKET_SIZE)
#define BUCKET_CAP 15
#define OP_FAIL -1
#define OP_SUCCESS 0
#define NOT_FOUND 1
#define COLLISON 2
#define OUT_OF_MEM 3
#define BUCKET_LOCKED 4
#define VAL_CORRUPT 5
#define DATA_HEADER 16
#define KEY_LEN 8
#define VAL_LEN 8
#define CRC_LEN 8
#define CACHE_LINE_SIZE 64
#define CACHE_LINE_MASK 63
