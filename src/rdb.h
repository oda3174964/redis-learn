/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __RDB_H
#define __RDB_H

#include <stdio.h>
#include "rio.h"

/* TBD: include only necessary headers. */
#include "server.h"

/* The current RDB version. When the format changes in a way that is no longer
 * backward compatible this number gets incremented. */
//RDB的版本
#define RDB_VERSION 9

/* Defines related to the dump file format. To store 32 bits lengths for short
 * keys requires a lot of space, so we check the most significant 2 bits of
 * the first byte to interpreter the length:
 *
 * 00|XXXXXX => if the two MSB are 00 the len is the 6 bits of this byte
 * 01|XXXXXX XXXXXXXX =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
 * 10|000000 [32 bit integer] => A full 32 bit len in net byte order will follow
 * 10|000001 [64 bit integer] => A full 64 bit len in net byte order will follow
 * 11|OBKIND this means: specially encoded object will follow. The six bits
 *           number specify the kind of object that follows.
 *           See the RDB_ENC_* defines.
 *
 * Lengths up to 63 are stored using a single byte, most DB keys, and may
 * values, will fit inside. */
#define RDB_6BITLEN 0  //6位长
#define RDB_14BITLEN 1  //14位长
#define RDB_32BITLEN 0x80  //32位长
#define RDB_64BITLEN 0x81  //64位长
#define RDB_ENCVAL 3  //编码值
#define RDB_LENERR UINT64_MAX  //错误值

/* When a length of a string object stored on disk has the first two bits
 * set, the remaining six bits specify a special encoding for the object
 * accordingly to the following defines: */
// 8位有符号整数
#define RDB_ENC_INT8 0        /* 8 bit signed integer */
// 16位有符号整数
#define RDB_ENC_INT16 1       /* 16 bit signed integer */
// 32位有符号整数
#define RDB_ENC_INT32 2       /* 32 bit signed integer */
// LZF压缩过的字符串
#define RDB_ENC_LZF 3         /* string compressed with FASTLZ */

/* Map object types to RDB object types. Macros starting with OBJ_ are for
 * memory storage and may change. Instead RDB types must be fixed because
 * we store them on disk. */
//字符串类型
#define RDB_TYPE_STRING 0
//列表类型
#define RDB_TYPE_LIST   1
//集合类型
#define RDB_TYPE_SET    2
//有序集合类型
#define RDB_TYPE_ZSET   3
//哈希类型
#define RDB_TYPE_HASH   4
#define RDB_TYPE_ZSET_2 5 /* ZSET version 2 with doubles stored in binary. */
#define RDB_TYPE_MODULE 6
#define RDB_TYPE_MODULE_2 7 /* Module value with annotations for parsing without
                               the generating module being loaded. */
/* NOTE: WHEN ADDING NEW RDB TYPE, UPDATE rdbIsObjectType() BELOW */

/* Object types for encoded objects. */
#define RDB_TYPE_HASH_ZIPMAP    9
//列表对象的ziplist编码类型
#define RDB_TYPE_LIST_ZIPLIST  10
//集合对象的intset编码类型
#define RDB_TYPE_SET_INTSET    11
//有序集合的ziplist编码类型
#define RDB_TYPE_ZSET_ZIPLIST  12
//哈希对象的ziplist编码类型
#define RDB_TYPE_HASH_ZIPLIST  13
//列表对象的quicklist编码类型
#define RDB_TYPE_LIST_QUICKLIST 14
#define RDB_TYPE_STREAM_LISTPACKS 15
/* NOTE: WHEN ADDING NEW RDB TYPE, UPDATE rdbIsObjectType() BELOW */

/* Test if a type is an object type. */
// 测试t是否是一个对象的编码类型
#define rdbIsObjectType(t) ((t >= 0 && t <= 7) || (t >= 9 && t <= 15))

/* Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
#define RDB_OPCODE_MODULE_AUX 247   /* Module auxiliary data. */
#define RDB_OPCODE_IDLE       248   /* LRU idle time. */
#define RDB_OPCODE_FREQ       249   /* LFU frequency. */
//辅助标识
#define RDB_OPCODE_AUX        250   /* RDB aux field. */
//提示调整哈希表大小的操作码
#define RDB_OPCODE_RESIZEDB   251   /* Hash table resize hint. */
//过期时间毫秒
#define RDB_OPCODE_EXPIRETIME_MS 252    /* Expire time in milliseconds. */
//过期时间秒
#define RDB_OPCODE_EXPIRETIME 253       /* Old expire time in seconds. */
//选择数据库的操作
#define RDB_OPCODE_SELECTDB   254   /* DB number of the following keys. */
//EOF码
#define RDB_OPCODE_EOF        255   /* End of the RDB file. */

/* Module serialized values sub opcodes */
#define RDB_MODULE_OPCODE_EOF   0   /* End of module value. */
#define RDB_MODULE_OPCODE_SINT  1   /* Signed integer. */
#define RDB_MODULE_OPCODE_UINT  2   /* Unsigned integer. */
#define RDB_MODULE_OPCODE_FLOAT 3   /* Float. */
#define RDB_MODULE_OPCODE_DOUBLE 4  /* Double. */
#define RDB_MODULE_OPCODE_STRING 5  /* String. */

/* rdbLoad...() functions flags. */
#define RDB_LOAD_NONE   0
#define RDB_LOAD_ENC    (1<<0)
#define RDB_LOAD_PLAIN  (1<<1)
#define RDB_LOAD_SDS    (1<<2)

/* flags on the purpose of rdb save or load */
#define RDBFLAGS_NONE 0                 /* No special RDB loading. */
#define RDBFLAGS_AOF_PREAMBLE (1<<0)    /* Load/save the RDB as AOF preamble. */
#define RDBFLAGS_REPLICATION (1<<1)     /* Load/save for SYNC. */
#define RDBFLAGS_ALLOW_DUP (1<<2)       /* Allow duplicated keys when loading.*/

// 将长度为1的type字符写到rdb中
int rdbSaveType(rio *rdb, unsigned char type);
// 从rdb中载入1字节的数据保存在type中，并返回其type
int rdbLoadType(rio *rdb);
int rdbSaveTime(rio *rdb, time_t t);
// 从rio读出一个时间，单位为秒，长度为4字节
time_t rdbLoadTime(rio *rdb);
// 将一个被编码的长度写入到rio中，返回保存编码后的len需要的字节数
int rdbSaveLen(rio *rdb, uint64_t len);
int rdbSaveMillisecondTime(rio *rdb, long long t);
long long rdbLoadMillisecondTime(rio *rdb, int rdbver);
// 返回一个从rio读出的len值，如果该len值不是整数，而是被编码后的值，那么将isencoded设置为1
uint64_t rdbLoadLen(rio *rdb, int *isencoded);
int rdbLoadLenByRef(rio *rdb, int *isencoded, uint64_t *lenptr);
// 将对象o的类型写到rio中
int rdbSaveObjectType(rio *rdb, robj *o);
// 从rio中读出一个类型并返回
int rdbLoadObjectType(rio *rdb);
// 将指定的RDB文件读到数据库中
int rdbLoad(char *filename, rdbSaveInfo *rsi, int rdbflags);
// 后台进行RDB持久化BGSAVE操作
int rdbSaveBackground(char *filename, rdbSaveInfo *rsi);
// fork一个子进程将rdb写到状态为等待BGSAVE开始的从节点的socket中
int rdbSaveToSlavesSockets(rdbSaveInfo *rsi);
// 删除临时文件，当BGSAVE执行被中断时使用
void rdbRemoveTempFile(pid_t childpid, int from_signal);
// 将数据库保存在磁盘上，返回C_OK成功，否则返回C_ERR
int rdbSave(char *filename, rdbSaveInfo *rsi);
// 将一个对象写到rio中，出错返回-1，成功返回写的字节数
ssize_t rdbSaveObject(rio *rdb, robj *o, robj *key);
// 返回一个对象的长度，通过写入的方式
size_t rdbSavedObjectLen(robj *o, robj *key);
// 从rio中读出一个rdbtype类型的对象，成功返回新对象地址，否则返回NULL
robj *rdbLoadObject(int type, rio *rdb, sds key);
// 当BGSAVE 完成RDB文件，要么发送给从节点，要么保存到磁盘，调用正确的处理
void backgroundSaveDoneHandler(int exitcode, int bysignal);
// 将一个键对象，值对象，过期时间，和类型写入到rio中，出错返回-1，成功返回1，键过期返回0
int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val, long long expiretime);
ssize_t rdbSaveSingleModuleAux(rio *rdb, int when, moduleType *mt);
robj *rdbLoadCheckModuleValue(rio *rdb, char *modulename);
// 从rio中读出一个字符串编码的对象
robj *rdbLoadStringObject(rio *rdb);
ssize_t rdbSaveStringObject(rio *rdb, robj *obj);
ssize_t rdbSaveRawString(rio *rdb, unsigned char *s, size_t len);
void *rdbGenericLoadStringObject(rio *rdb, int flags, size_t *lenptr);
int rdbSaveBinaryDoubleValue(rio *rdb, double val);
int rdbLoadBinaryDoubleValue(rio *rdb, double *val);
int rdbSaveBinaryFloatValue(rio *rdb, float val);
int rdbLoadBinaryFloatValue(rio *rdb, float *val);
int rdbLoadRio(rio *rdb, int rdbflags, rdbSaveInfo *rsi);
int rdbSaveRio(rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi);
rdbSaveInfo *rdbPopulateSaveInfo(rdbSaveInfo *rsi);

#endif
