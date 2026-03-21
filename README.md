# redis-rdb-byte-parser

Redis RDB 文件字节级解析器  
Byte-level parser for Redis RDB files

## 介绍 / Introduction

从零手写实现的 Redis RDB 解析工具，以底层字节流方式解析 RDB 文件，主要用于学习、研究 Redis 持久化格式。

A lightweight Redis RDB parser implemented from scratch. It parses RDB files byte by byte for learning and research.

## 已支持功能 / Supported Features

- RDB 文件头解析 / RDB header parsing
- AUX 元信息字段 / AUX metadata fields
- SELECTDB 数据库选择 / SELECTDB database selection
- EXPIRETIME_MS 毫秒级过期时间 / Millisecond expiration
- IDLE 空闲时间 / IDLE time
- FREQ（LFU 访问热度）/ LFU frequency
- STRING 字符串类型 / STRING data type

## 编译与使用 / Compile & Usage

```bash
g++ main.cpp -o redis-rdb-byte-parser
./redis-rdb-byte-parser dump.rdb