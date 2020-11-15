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

#include "server.h"
#include <math.h> /* isnan(), isinf() */

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/
//检查字符串的长度是否超过指定的值
static int checkStringLength(client *c, long long size) {
    if (!(c->flags & CLIENT_MASTER) && size > server.proto_max_bulk_len) {
        addReplyError(c,"string exceeds maximum allowed size (proto-max-bulk-len)");
        return C_ERR; //超过则返回-1
    }
    return C_OK;
}

/* The setGenericCommand() function implements the SET operation with different
 * options and variants. This function is called in order to implement the
 * following commands: SET, SETEX, PSETEX, SETNX.
 *
 * 'flags' changes the behavior of the command (NX or XX, see below).
 *
 * 'expire' represents an expire to set in form of a Redis object as passed
 * by the user. It is interpreted according to the specified 'unit'.
 *
 * 'ok_reply' and 'abort_reply' is what the function will reply to the client
 * if the operation is performed, or when it is not because of NX or
 * XX flags.
 *
 * If ok_reply is NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used. */

#define OBJ_SET_NO_FLAGS 0
//在key不存在的情况下才会设置
#define OBJ_SET_NX (1<<0)          /* Set if key not exists. */
//在key存在的情况下才会设置
#define OBJ_SET_XX (1<<1)          /* Set if key exists. */
//以秒(s)为单位设置键的key过期时间
#define OBJ_SET_EX (1<<2)          /* Set if time in seconds is given */
//以毫秒(ms)为单位设置键的key过期时间
#define OBJ_SET_PX (1<<3)          /* Set if time in ms in given */
#define OBJ_SET_KEEPTTL (1<<4)     /* Set and keep the ttl */

//setGenericCommand()函数是以下命令: SET, SETEX, PSETEX, SETNX.的最底层实现
//flags 可以是NX或XX，由上面的宏提供
//expire 定义key的过期时间，格式由unit指定
//ok_reply和abort_reply保存着回复client的内容，NX和XX也会改变回复
//如果ok_reply为空，则使用 "+OK"
//如果abort_reply为空，则使用 "$-1"
void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    long long milliseconds = 0; /* initialized to avoid any harmness warning */
    //如果定义了key的过期时间
    if (expire) {
        //从expire对象中取出值，保存在milliseconds中，如果出错发送默认的信息给client
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != C_OK)
            return;
        // 如果过期时间小于等于0，则发送错误信息给client
        if (milliseconds <= 0) {
            addReplyErrorFormat(c,"invalid expire time in %s",c->cmd->name);
            return;
        }
        //如果unit的单位是秒，则需要转换为毫秒保存
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }

    //lookupKeyWrite函数是为执行写操作而取出key的值对象
    //如果设置了NX(不存在)，并且在数据库中 找到 该key，或者
    //设置了XX(存在)，并且在数据库中 没有找到 该key
    //回复abort_reply给client
    if ((flags & OBJ_SET_NX && lookupKeyWrite(c->db,key) != NULL) ||
        (flags & OBJ_SET_XX && lookupKeyWrite(c->db,key) == NULL))
    {
        addReply(c, abort_reply ? abort_reply : shared.null[c->resp]);
        return;
    }
    //在当前db设置键为key的值为val
    genericSetKey(c,c->db,key,val,flags & OBJ_SET_KEEPTTL,1);
    server.dirty++;//设置数据库为脏(dirty)，服务器每次修改一个key后，都会对脏键(dirty)增1
    if (expire) setExpire(c,c->db,key,mstime()+milliseconds);
    //发送"set"事件的通知，用于发布订阅模式，通知客户端接受发生的事件
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);

    //发送"expire"事件通知
    if (expire) notifyKeyspaceEvent(NOTIFY_GENERIC,
        "expire",key,c->db->id);

    //设置成功，则向客户端发送ok_reply
    addReply(c, ok_reply ? ok_reply : shared.ok);
}

/* SET key value [NX] [XX] [KEEPTTL] [EX <seconds>] [PX <milliseconds>] */
//SET命令
void setCommand(client *c) {
    int j;
    robj *expire = NULL;
    int unit = UNIT_SECONDS; //单位为秒
    int flags = OBJ_SET_NO_FLAGS; //初始化为0，表示默认为没有后面的[NX] [XX] [EX] [PX]参数

    //从第四个参数开始解析，
    for (j = 3; j < c->argc; j++) {
        char *a = c->argv[j]->ptr; //保存第四个参数的首地址

        //EX和PX 参数后要 一个时间参数，next保存时间参数的地址
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        //如果是 "nx" 或 "NX" 并且 flags没有设置 "XX" 的标志位
        if ((a[0] == 'n' || a[0] == 'N') &&
            (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
            !(flags & OBJ_SET_XX))
        {
            flags |= OBJ_SET_NX; //设置 "NX" 标志位
        } else if ((a[0] == 'x' || a[0] == 'X') && //如果是 "xx" 或 "XX" 并且 flags没有设置 "NX" 的标志位
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_NX))
        {
            flags |= OBJ_SET_XX; //设置 "XX" 标志位
        } else if (!strcasecmp(c->argv[j]->ptr,"KEEPTTL") &&
                   !(flags & OBJ_SET_EX) && !(flags & OBJ_SET_PX))
        {
            flags |= OBJ_SET_KEEPTTL;
        } else if ((a[0] == 'e' || a[0] == 'E') && //如果是 "ex" 或 "EX" 并且 flags没有设置 "PX" 的标志位，并且后面跟了时间
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_KEEPTTL) &&
                   !(flags & OBJ_SET_PX) && next)
        {
            flags |= OBJ_SET_EX; //设置 "EX" 标志位
            unit = UNIT_SECONDS; //EX 单位为秒
            expire = next; //保存时间值
            j++; //跳过时间参数的下标
        } else if ((a[0] == 'p' || a[0] == 'P') && //如果是 "px" 或 "PX" 并且 flags没有设置 "EX" 的标志位，并且后面跟了时间
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_KEEPTTL) &&
                   !(flags & OBJ_SET_EX) && next)
        {
            flags |= OBJ_SET_PX; //设置 "PX" 标志位
            unit = UNIT_MILLISECONDS; //PX 单位为毫秒
            expire = next; //保存时间值
            j++; //跳过时间参数的下标
        } else {
            //不是以上格式则回复client语法错误
            addReply(c,shared.syntaxerr);
            return;
        }
    }
    //对value进行最优的编码
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    //调用底层的setGenericCommand函数实现SET命令
    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

// SETNX 命令实现
void setnxCommand(client *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,OBJ_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

// SETEX 命令实现
void setexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

// PSETEX 命令实现
void psetexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

//GET 命令的底层实现
int getGenericCommand(client *c) {
    robj *o;
    //lookupKeyReadOrReply函数是为执行读操作而返回key的值对象，找到返回该对象，找不到会发送信息给client
    //如果key不存在直接，返回0表示GET命令执行成功
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return C_OK;
    //如果key的值的编码类型不是字符串对象
    if (o->type != OBJ_STRING) {
        addReply(c,shared.wrongtypeerr); //返回类型错误的信息给client，返回-1表示GET命令执行失败
        return C_ERR;
    } else {
        addReplyBulk(c,o); //返回之前找到的对象作为回复给client，返回0表示GET命令执行成功
        return C_OK;
    }
}

//调用getGenericCommand实现GET命令
void getCommand(client *c) {
    getGenericCommand(c);
}

//GETSET 命令的实现
void getsetCommand(client *c) {
    //先用GET命令得到val对象返回发给client
    if (getGenericCommand(c) == C_ERR) return;
    //在对新的val进行优化编码
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    //设置key的值为新val
    setKey(c,c->db,c->argv[1],c->argv[2]);
    //发送一个"set"事件通知
    notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[1],c->db->id);
    //将脏键加1
    server.dirty++;
}

//SETRANGE 命令的实现
void setrangeCommand(client *c) {
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr; //获得要设置的value值

    //从offset对象中取出long类型的值，保存在offset中，不成功直接发送错误信息给client
    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != C_OK)
        return;

    //如果偏移量小于0，则回复错误信息给client
    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    //以写操作的取出key的值对象保存在o中
    o = lookupKeyWrite(c->db,c->argv[1]);

    //key没有找到
    if (o == NULL) {
        /* Return 0 when setting nothing on a non-existing string */
        //且当value为空，返回0，什么也不做
        if (sdslen(value) == 0) {
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        //检查字符串长度是否超过设置，超过会给client发送一个错误信息
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        //如果key在当前数据库中不存在，且命令中制定了value，且value值的长度没有超过redis的限制
        //创建一个新的key字符串对象
        o = createObject(OBJ_STRING,sdsnewlen(NULL, offset+sdslen(value)));
        //设置新键key的value
        dbAdd(c->db,c->argv[1],o);
    } else { //key已经存在于数据库
        size_t olen;

        /* Key exists, check type */
        //检查value对象是否是字符串类型的对象，是返回0，不是返回1且发送错误信息
        if (checkType(c,o,OBJ_STRING))
            return;

        /* Return existing string length when setting nothing */
        //返回value的长度
        olen = stringObjectLen(o);
        //value长度为0，发送0给client
        if (sdslen(value) == 0) {
            addReplyLongLong(c,olen);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        //检查字符串长度是否超过设置，超过会给client发送一个错误信息
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        /* Create a copy when the object is shared or encoded. */
        //因为要根据value修改key的值，因此如果key原来的值是共享的，需要解除共享，新创建一个值对象与key组对
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    //如果value对象的长度大于0
    if (sdslen(value) > 0) {
        //将值对象扩展offset+sdslen(value)的长度，扩充的内存使用"\0"填充
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));
        //将value填入到偏移量起始的地址上
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        //当数据库的键被改动，则会调用该函数发送信号
        signalModifiedKey(c,c->db,c->argv[1]);

        //发送"setrange"时间通知
        notifyKeyspaceEvent(NOTIFY_STRING,
            "setrange",c->argv[1],c->db->id);
        //将脏键加1
        server.dirty++;
    }
    //发送新的vlaue值给client
    addReplyLongLong(c,sdslen(o->ptr));
}

//GETRANGE 命令的实现
void getrangeCommand(client *c) {
    robj *o;
    long long start, end;
    char *str, llbuf[32];
    size_t strlen;

    //将起始下标以long long 类型保存到start中
    if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
        return;
    //将起始下标以long long 类型保存到end中
    if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
        return;
    //以读操作的取出key的值对象返回到o中
    //如果value对象不是字符串对象或key对象不存在，直接返回
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;

    //如果value对象的编码为整型类型
    if (o->encoding == OBJ_ENCODING_INT) {
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr); //将整型转换为字符串型，并保存字符串长度
    } else {
        str = o->ptr;
        strlen = sdslen(str); //保存字符串长度
    }

    /* Convert negative indexes */
    //参数范围出错，发送错误信息
    if (start < 0 && end < 0 && start > end) {
        addReply(c,shared.emptybulk);
        return;
    }
    //将负数下标转换为正数形式
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    //如果转换后的下标仍为负数，则设置为0
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    //end不能超过字符串长度
    if ((unsigned long long)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    if (start > end || strlen == 0) {
        addReply(c,shared.emptybulk); //参数范围出错，发送错误信息
    } else {
        //发送给定范围内的内容给client
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

// MGET 命令的实现
void mgetCommand(client *c) {
    int j;
    //发送key的个数给client
    addReplyArrayLen(c,c->argc-1);

    //从下标为1的key，遍历argc次
    for (j = 1; j < c->argc; j++) {
        //以读操作取出key的value对象
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            //没找到当前的key的value，发送空信息
            addReplyNull(c);
        } else {
            //找到的value对象不是字符串类型的对象
            if (o->type != OBJ_STRING) {
                addReplyNull(c); //发送空信息
            } else {
                addReplyBulk(c,o); //如果是字符串类型的对象，则发送value对象给client
            }
        }
    }
}

// MSET 的底层实现
void msetGenericCommand(client *c, int nx) {
    int j;

    //如果参数个数不是奇数个，则发送错误信息给client
    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }

    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set anything if at least one key already exists. */
    //如果制定NX标志
    if (nx) {
        for (j = 1; j < c->argc; j += 2) { //遍历所有的key对象
            //以写操作取出key的value对象，查找所有的key对象是否存在，并计数存在的key对象
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {  
                addReply(c, shared.czero);
                return;
            }
        }
    }

    //没有制定NX标志，遍历所有的key对象
    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]); //对value对象优化编码
        setKey(c,c->db,c->argv[j],c->argv[j+1]); //设置key的值为value
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id); //发送"set"事件通知
    }
    //更新脏键
    server.dirty += (c->argc-1)/2;
    //MSETNX返回1，MSET返回ok
    addReply(c, nx ? shared.cone : shared.ok);
}

//MSET命令的实现
void msetCommand(client *c) {
    msetGenericCommand(c,0);
}

//MSETNX命令的实现
void msetnxCommand(client *c) {
    msetGenericCommand(c,1);
}

//INCR和DECR命令的底层实现
void incrDecrCommand(client *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    o = lookupKeyWrite(c->db,c->argv[1]); //以写操作获取key的value对象
    //找到了value对象但是value对象不是字符串类型，直接返回
    if (o != NULL && checkType(c,o,OBJ_STRING)) return;

    //将字符串类型的value转换为longlong类型保存在value中
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != C_OK) return;

    oldvalue = value; //备份旧的value

    //如果incr超出longlong类型所能表示的范围，发送错误信息
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr; //计算新的value值

    //value对象目前非共享，编码为整型类型，且新value值不在共享范围，且value处于long类型所表示的范围内
    if (o && o->refcount == 1 && o->encoding == OBJ_ENCODING_INT &&
        (value < 0 || value >= OBJ_SHARED_INTEGERS) &&
        value >= LONG_MIN && value <= LONG_MAX)
    {
        new = o;
        o->ptr = (void*)((long)value); //设置vlaue对象的值
    } else {
        //当不满足以上任意条件，则新创建一个字符串对象
        new = createStringObjectFromLongLongForValue(value);

        //如果之前的value对象存在
        if (o) {
            dbOverwrite(c->db,c->argv[1],new); //用new对象去重写key的值
        } else {
            //如果之前的value不存在，将key和new组成新的key-value对
            dbAdd(c->db,c->argv[1],new);
        }
    }

    //当数据库的键被改动，则会调用该函数发送信号
    signalModifiedKey(c,c->db,c->argv[1]);
    //发送"incrby"事件通知
    notifyKeyspaceEvent(NOTIFY_STRING,"incrby",c->argv[1],c->db->id);
    //设置脏键
    server.dirty++;
    //回复信息给client
    addReply(c,shared.colon);
    addReply(c,new);
    addReply(c,shared.crlf);
}

//INCR命令实现
void incrCommand(client *c) {
    incrDecrCommand(c,1);
}

//DECR命令实现
void decrCommand(client *c) {
    incrDecrCommand(c,-1);
}

//INCRBY命令实现
void incrbyCommand(client *c) {
    long long incr;

    //将decrement对象的值转换为long long 类型保存在incr中
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    incrDecrCommand(c,incr);
}

//DECRBY命令实现
void decrbyCommand(client *c) {
    long long incr;
    //将decrement对象的值转换为long long 类型保存在incr中
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    //取incr的相反数
    incrDecrCommand(c,-incr);
}

// INCRBYFLOAT key increment
// INCRBYFLOAT命令的实现
void incrbyfloatCommand(client *c) {
    long double incr, value;
    robj *o, *new, *aux1, *aux2;

    //以写操作获取key的value对象
    o = lookupKeyWrite(c->db,c->argv[1]);

    //找到了value对象但是value对象不是字符串类型，直接返回
    if (o != NULL && checkType(c,o,OBJ_STRING)) return;

    //将key的值转换为long double类型保存在value中
    //将increment对象的值保存在incr中
    //两者任一不成功，则直接返回并且发送错误信息
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != C_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != C_OK)
        return;

    //计算出新的value值
    value += incr;
    //如果value不是数字或者value是正无穷或负无穷，发送错误信息
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }
    //创建一个字符串类型的value对象
    new = createStringObjectFromLongDouble(value,1);
    //如果之前的value对象存在
    if (o)
        dbOverwrite(c->db,c->argv[1],new); //用new对象去重写key的值
    else
        dbAdd(c->db,c->argv[1],new); //如果之前的value不存在，将key和new组成新的key-value对

    //当数据库的键被改动，则会调用该函数发送信号
    signalModifiedKey(c,c->db,c->argv[1]);
    //发送"incrbyfloat"事件通知
    notifyKeyspaceEvent(NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);
    //设置脏键
    server.dirty++;
    //将新的value对象返回给client
    addReplyBulk(c,new);

    /* Always replicate INCRBYFLOAT as a SET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    //防止因为不同的浮点精度和格式化造成AOF重启时的不一致
    //创建一个值为"SET"的字符串对象
    aux1 = createStringObject("SET",3);
    //将client中的"INCRBYFLOAT"替换为"SET"命令
    rewriteClientCommandArgument(c,0,aux1);
    //释放aux对象
    decrRefCount(aux1);
    //将client中的"increment"参数替换为new对象中的值
    rewriteClientCommandArgument(c,2,new);
    aux2 = createStringObject("KEEPTTL",7);
    rewriteClientCommandArgument(c,3,aux2);
    decrRefCount(aux2);
}

// APPEND key value
// APPEND命令的实现
void appendCommand(client *c) {
    size_t totlen;
    robj *o, *append;

    //以写操作获取key的value对象
    o = lookupKeyWrite(c->db,c->argv[1]);
    //如果没有获取到vlaue，则要创建一个
    if (o == NULL) {
        /* Create the key */
        c->argv[2] = tryObjectEncoding(c->argv[2]); //对参数value进行优化编码
        dbAdd(c->db,c->argv[1],c->argv[2]); //将key和value组成新的key-value对
        incrRefCount(c->argv[2]); //增加value的引用计数
        totlen = stringObjectLen(c->argv[2]); //返回vlaue的长度
    } else { //获取到value
        /* Key exists, check type */
       
        //如果value不是字符串类型的对象直接返回
        if (checkType(c,o,OBJ_STRING))
            return;

        /* "append" is an argument, so always an sds */
        //获得追加的值对象
        append = c->argv[2];
        //计算追加后的长度
        totlen = stringObjectLen(o)+sdslen(append->ptr);
        //如果追加后的长度超出范围，则返回
        if (checkStringLength(c,totlen) != C_OK)
            return;

        /* Append the value */
        //因为要根据value修改key的值，因此如果key原来的值是共享的，需要解除共享，新创建一个值对象与key组对
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        //将vlaue对象的值后面追加上append的值
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        //计算出追加后值的长度
        totlen = sdslen(o->ptr);
    }
    signalModifiedKey(c,c->db,c->argv[1]); //当数据库的键被改动，则会调用该函数发送信号
    //发送"append"事件通知
    notifyKeyspaceEvent(NOTIFY_STRING,"append",c->argv[1],c->db->id);
    server.dirty++;
    //发送追加后value的长度给client
    addReplyLongLong(c,totlen);
}

// STRLEN命令的实现
void strlenCommand(client *c) {
    robj *o;
    //以读操作取出key对象的value对象，并且检查value是否是字符串对象
    //如果没找到value或者不是字符串对象则直接返回
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;
    //发送字符串的长度给client
    addReplyLongLong(c,stringObjectLen(o));
}


/* STRALGO -- Implement complex algorithms on strings.
 *
 * STRALGO <algorithm> ... arguments ... */
void stralgoLCS(client *c);     /* This implements the LCS algorithm. */
void stralgoCommand(client *c) {
    /* Select the algorithm. */
    if (!strcasecmp(c->argv[1]->ptr,"lcs")) {
        stralgoLCS(c);
    } else {
        addReply(c,shared.syntaxerr);
    }
}

/* STRALGO <algo> [IDX] [MINMATCHLEN <len>] [WITHMATCHLEN]
 *     STRINGS <string> <string> | KEYS <keya> <keyb>
 */
void stralgoLCS(client *c) {
    uint32_t i, j;
    long long minmatchlen = 0;
    sds a = NULL, b = NULL;
    int getlen = 0, getidx = 0, withmatchlen = 0;
    robj *obja = NULL, *objb = NULL;

    for (j = 2; j < (uint32_t)c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1) - j;

        if (!strcasecmp(opt,"IDX")) {
            getidx = 1;
        } else if (!strcasecmp(opt,"LEN")) {
            getlen = 1;
        } else if (!strcasecmp(opt,"WITHMATCHLEN")) {
            withmatchlen = 1;
        } else if (!strcasecmp(opt,"MINMATCHLEN") && moreargs) {
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&minmatchlen,NULL)
                != C_OK) goto cleanup;
            if (minmatchlen < 0) minmatchlen = 0;
            j++;
        } else if (!strcasecmp(opt,"STRINGS") && moreargs > 1) {
            if (a != NULL) {
                addReplyError(c,"Either use STRINGS or KEYS");
                goto cleanup;
            }
            a = c->argv[j+1]->ptr;
            b = c->argv[j+2]->ptr;
            j += 2;
        } else if (!strcasecmp(opt,"KEYS") && moreargs > 1) {
            if (a != NULL) {
                addReplyError(c,"Either use STRINGS or KEYS");
                goto cleanup;
            }
            obja = lookupKeyRead(c->db,c->argv[j+1]);
            objb = lookupKeyRead(c->db,c->argv[j+2]);
            if ((obja && obja->type != OBJ_STRING) ||
                (objb && objb->type != OBJ_STRING))
            {
                addReplyError(c,
                    "The specified keys must contain string values");
                /* Don't cleanup the objects, we need to do that
                 * only after callign getDecodedObject(). */
                obja = NULL;
                objb = NULL;
                goto cleanup;
            }
            obja = obja ? getDecodedObject(obja) : createStringObject("",0);
            objb = objb ? getDecodedObject(objb) : createStringObject("",0);
            a = obja->ptr;
            b = objb->ptr;
            j += 2;
        } else {
            addReply(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    /* Complain if the user passed ambiguous parameters. */
    if (a == NULL) {
        addReplyError(c,"Please specify two strings: "
                        "STRINGS or KEYS options are mandatory");
        goto cleanup;
    } else if (getlen && getidx) {
        addReplyError(c,
            "If you want both the length and indexes, please "
            "just use IDX.");
        goto cleanup;
    }

    /* Compute the LCS using the vanilla dynamic programming technique of
     * building a table of LCS(x,y) substrings. */
    uint32_t alen = sdslen(a);
    uint32_t blen = sdslen(b);

    /* Setup an uint32_t array to store at LCS[i,j] the length of the
     * LCS A0..i-1, B0..j-1. Note that we have a linear array here, so
     * we index it as LCS[j+(blen+1)*j] */
    uint32_t *lcs = zmalloc((alen+1)*(blen+1)*sizeof(uint32_t));
    #define LCS(A,B) lcs[(B)+((A)*(blen+1))]

    /* Start building the LCS table. */
    for (uint32_t i = 0; i <= alen; i++) {
        for (uint32_t j = 0; j <= blen; j++) {
            if (i == 0 || j == 0) {
                /* If one substring has length of zero, the
                 * LCS length is zero. */
                LCS(i,j) = 0;
            } else if (a[i-1] == b[j-1]) {
                /* The len LCS (and the LCS itself) of two
                 * sequences with the same final character, is the
                 * LCS of the two sequences without the last char
                 * plus that last char. */
                LCS(i,j) = LCS(i-1,j-1)+1;
            } else {
                /* If the last character is different, take the longest
                 * between the LCS of the first string and the second
                 * minus the last char, and the reverse. */
                uint32_t lcs1 = LCS(i-1,j);
                uint32_t lcs2 = LCS(i,j-1);
                LCS(i,j) = lcs1 > lcs2 ? lcs1 : lcs2;
            }
        }
    }

    /* Store the actual LCS string in "result" if needed. We create
     * it backward, but the length is already known, we store it into idx. */
    uint32_t idx = LCS(alen,blen);
    sds result = NULL;        /* Resulting LCS string. */
    void *arraylenptr = NULL; /* Deffered length of the array for IDX. */
    uint32_t arange_start = alen, /* alen signals that values are not set. */
             arange_end = 0,
             brange_start = 0,
             brange_end = 0;

    /* Do we need to compute the actual LCS string? Allocate it in that case. */
    int computelcs = getidx || !getlen;
    if (computelcs) result = sdsnewlen(SDS_NOINIT,idx);

    /* Start with a deferred array if we have to emit the ranges. */
    uint32_t arraylen = 0;  /* Number of ranges emitted in the array. */
    if (getidx) {
        addReplyMapLen(c,2);
        addReplyBulkCString(c,"matches");
        arraylenptr = addReplyDeferredLen(c);
    }

    i = alen, j = blen;
    while (computelcs && i > 0 && j > 0) {
        int emit_range = 0;
        if (a[i-1] == b[j-1]) {
            /* If there is a match, store the character and reduce
             * the indexes to look for a new match. */
            result[idx-1] = a[i-1];

            /* Track the current range. */
            if (arange_start == alen) {
                arange_start = i-1;
                arange_end = i-1;
                brange_start = j-1;
                brange_end = j-1;
            } else {
                /* Let's see if we can extend the range backward since
                 * it is contiguous. */
                if (arange_start == i && brange_start == j) {
                    arange_start--;
                    brange_start--;
                } else {
                    emit_range = 1;
                }
            }
            /* Emit the range if we matched with the first byte of
             * one of the two strings. We'll exit the loop ASAP. */
            if (arange_start == 0 || brange_start == 0) emit_range = 1;
            idx--; i--; j--;
        } else {
            /* Otherwise reduce i and j depending on the largest
             * LCS between, to understand what direction we need to go. */
            uint32_t lcs1 = LCS(i-1,j);
            uint32_t lcs2 = LCS(i,j-1);
            if (lcs1 > lcs2)
                i--;
            else
                j--;
            if (arange_start != alen) emit_range = 1;
        }

        /* Emit the current range if needed. */
        uint32_t match_len = arange_end - arange_start + 1;
        if (emit_range) {
            if (minmatchlen == 0 || match_len >= minmatchlen) {
                if (arraylenptr) {
                    addReplyArrayLen(c,2+withmatchlen);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,arange_start);
                    addReplyLongLong(c,arange_end);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,brange_start);
                    addReplyLongLong(c,brange_end);
                    if (withmatchlen) addReplyLongLong(c,match_len);
                    arraylen++;
                }
            }
            arange_start = alen; /* Restart at the next match. */
        }
    }

    /* Signal modified key, increment dirty, ... */

    /* Reply depending on the given options. */
    if (arraylenptr) {
        addReplyBulkCString(c,"len");
        addReplyLongLong(c,LCS(alen,blen));
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else if (getlen) {
        addReplyLongLong(c,LCS(alen,blen));
    } else {
        addReplyBulkSds(c,result);
        result = NULL;
    }

    /* Cleanup. */
    sdsfree(result);
    zfree(lcs);

cleanup:
    if (obja) decrRefCount(obja);
    if (objb) decrRefCount(objb);
    return;
}

