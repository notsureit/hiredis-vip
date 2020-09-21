//
// Created by liutao on 2020/7/14.
//
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>

#include "hircluster.h"
#include "adapters/libevent.h"

#include "async.h"

#define REDIS_REPLY_MAXSIZE_ARRAY           32
#define REDIS_REPLY_MAXLENGTH_ITEM           1024

typedef enum {
    ENUM_REDISREAD_BROADCAST = 0,
    ENUM_REDISREAD_DIRECT
} ENUM_REDISREAD_METHOD;

struct SRunParam {
    //独立参数
    char myname[32]; //本运行实例的名字，同一个运行网络内唯一，不可变

    //redis消费参数
    char redis_topic[32]; //XXX:没啥用，要删除
    char redis_broker_list[128]; //redis服务列表，采用逗号隔开
    char redis_streamname_read[32]; //上行，也就是读stream名字
    char redis_streamname_write[32]; //下行，也就是写stream名字
} runParams = {
        "liutao",
        "toplion",
        "192.168.80.140:6379,192.168.80.141:6379,192.168.80.142:6379",
        "iot/inform", "iot/action"
};;

static void __inline xreadStartAsync(redisClusterAsyncContext *c, const char *timesign);

static void __inline xdirectreadStartAsync(redisClusterAsyncContext *c, const char *timesign);

static void xreadCallback(struct redisClusterAsyncContext *c, void *r, void *privdata);

#define MAX_LEN_CACHLINE (1024) //单行缓冲区的长度
#define LEN_CACHEARRAYITEM (1024*16*4) //缓冲区的行数
static char CacheResult[LEN_CACHEARRAYITEM][MAX_LEN_CACHLINE]; //为了方便自定义函数返回字符串缓冲区使用的缓冲池
static size_t idxCacheResult = 0; //当前缓冲池可用指针下标
pthread_mutex_t mutexofDisplay = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexofCacheResult = PTHREAD_MUTEX_INITIALIZER;

/**
 * 函数：getCacheArrayIndex
 * 用途：获得当前可用的内部字符串缓冲块，并且把内部下标计数器向下移动
 * 返回值：
 *       当前可用的字符串缓冲区
 * 注意事项：
 * 		 使用本函数的前提，申请的内存较小，且会快速释放，长期使用的内存一定不要使用，会被后续的请求覆盖内容
 * 		 申请的内存会被自动清零
 */
void *getCacheBuffer(const size_t lengthwant) {
    char *ptresult = NULL;
    pthread_mutex_lock(&mutexofCacheResult);
    do {
        size_t iCurrentCache = idxCacheResult;
        size_t blockcnt;

        //计算申请的内存需要占用的静态数组成员个数
        if (lengthwant > MAX_LEN_CACHLINE) {
            blockcnt = (lengthwant / MAX_LEN_CACHLINE) + ((lengthwant % MAX_LEN_CACHLINE) ? 1 : 0);
        } else {
            blockcnt = 1;
        }

        if (blockcnt > LEN_CACHEARRAYITEM)  //如果申请的内存尺寸超过内部缓冲区所能提供的，直接返回null
        {
            fprintf(stderr, "Cache Index: %lu\nblockcnt(%lu) exceeds max (%lu)",
                     iCurrentCache, blockcnt, LEN_CACHEARRAYITEM);
            break;
        } else if (blockcnt + iCurrentCache >= LEN_CACHEARRAYITEM) { //如果申请的内存超过了高边界，将强制设置为底边界0（可能会造成数据丢失）
            iCurrentCache = 0;
            idxCacheResult = blockcnt;
        } else
            idxCacheResult = (idxCacheResult + blockcnt) % LEN_CACHEARRAYITEM; //采用取余数的方式实现静态数组下标的变换

        memset((char *) CacheResult[iCurrentCache], 0x00,
               blockcnt * MAX_LEN_CACHLINE); //将申请到的静态数组数据清零，不可隐去，外部调用可能会有字符串操作
        ptresult = (char *) CacheResult[iCurrentCache];
    } while (0);
    pthread_mutex_unlock(&mutexofCacheResult);
    return ptresult;
}

/**
 * 用途：获得字符串缓冲区
 * 参数：
 * 	str：初始化字符串
 */
char *getCacheString(const char *str) {
    char *ptresult = (char *) getCacheBuffer(strlen(str) + 1);
    if (ptresult)
        strcpy(ptresult, str);
    return ptresult;
}

char *getShortCacheString(const char *str, const ssize_t size) {
    if (size < 0)
        return NULL;
    else {
        char *ptresult = (char *) getCacheBuffer(size + 1);
        if (size == 0)
            ptresult[0] = 0x00;
        else if (ptresult)
            strncpy(ptresult, str, size);
        return ptresult;
    }
}

/**
 * 用途：什么也不做，为了方便使用malloc/free替换
 */
void putCacheBuffer(void *buffer) {
    if (!buffer)
        return;
//    pthread_mutex_lock(&mutexofCacheResult);
//    pthread_mutex_unlock(&mutexofCacheResult);
}

/**
 * 创建字符串的副本
 * @param key
 * @param maxlenofvalue
 * @return
 */
const char *getString(const char *key, const size_t maxlenofvalue) {
    char *buf = (char *) getCacheBuffer(maxlenofvalue + 1);
    memcpy(buf, key, maxlenofvalue);
    return buf;
}


/**
 * 递归的方式打印redis的reply内容
 * @param keystr
 * @param reply
 */
void printRedisReply(const char *title, redisReply *reply) {
    switch (reply->type) {
        case REDIS_REPLY_INTEGER:
            fprintf(stderr, "reply[%s]: %d\n", title, reply->integer);
            break;
        case REDIS_REPLY_ARRAY: {
            char szDisplay[128] = {};
            for (size_t i = 0; i < reply->elements; i++) {
                snprintf(szDisplay, sizeof(szDisplay) - 1, "%s:%lu", title, i + 1);
                printRedisReply(szDisplay, reply->element[i]);
            }
        }
            break;
        case REDIS_REPLY_STRING:
            fprintf(stderr, "reply[%s]: %s\n", title, reply->str);
            break;
        case REDIS_REPLY_STATUS:
            fprintf(stderr, "reply[%s]: %s\n", title, reply->str);
            break;
        case REDIS_REPLY_ERROR:
            fprintf(stderr, "reply[%s]错误: %s!\n", title, reply->str);
            break;
        default:
            fprintf(stderr, "reply[%s] unsupported!\n", title);
    }
}

/**
 * 填充rtnArray的第一个非空指针为传入的字符串
 * 另外，为啥刚写不久的代码看起来这么陌生。。。注释要早写啊！
 * @param rtnArray
 * @param stringbuffer
 * @return
 */
static __inline int fillReturnArray(const char **rtnArray, const char *stringbuffer) {
    int icounter;
    for (icounter = 0; icounter < REDIS_REPLY_MAXSIZE_ARRAY; icounter++) {
        if (rtnArray[icounter] == NULL) {
            rtnArray[icounter] = stringbuffer;
            break;
        }
    }
    return icounter;
}

/**
 * 解析hiredis返回的信息为一个字符串数组的数组
 * @param reply
 * @return
 */
const char **parseRedisReply(redisReply *reply, const char **rtnArray) {
    if (rtnArray == NULL)
        rtnArray = (const char **) getCacheBuffer(sizeof(const char *) * REDIS_REPLY_MAXSIZE_ARRAY);

    char *szBuffer = NULL;
    switch (reply->type) {
        case REDIS_REPLY_INTEGER:
            szBuffer = (char *) getCacheBuffer(REDIS_REPLY_MAXLENGTH_ITEM);
            snprintf(szBuffer, REDIS_REPLY_MAXLENGTH_ITEM, "%lld", reply->integer);
            fillReturnArray(rtnArray, szBuffer);
            break;
        case REDIS_REPLY_ARRAY:
            for (size_t i = 0; i < reply->elements; i++) {
                parseRedisReply(reply->element[i], rtnArray);
            }
            break;
        case REDIS_REPLY_STRING:
            szBuffer = (char *) getCacheString(reply->str);
            fillReturnArray(rtnArray, szBuffer);
            break;
        case REDIS_REPLY_STATUS:
            szBuffer = (char *) getCacheString(reply->str);
            fillReturnArray(rtnArray, szBuffer);
            break;
        case REDIS_REPLY_ERROR:
            szBuffer = (char *) getCacheString(reply->str);
            fillReturnArray(rtnArray, szBuffer);
            break;
        default:
            szBuffer = (char *) getCacheBuffer(REDIS_REPLY_MAXLENGTH_ITEM);
            snprintf(szBuffer, REDIS_REPLY_MAXLENGTH_ITEM, "reply type[%d] unsupported!\n", reply->type);
            fillReturnArray(rtnArray, szBuffer);
    }
    return rtnArray;
}

int sendBroadcast(struct redisClusterAsyncContext *c, const char *command) {

    int irtn = redisClusterAsyncCommand(c, xreadCallback,
                                        NULL,
                                        "XADD %s * data %s",
                                        runParams.redis_streamname_write, command);
    return irtn;
}

static void xreadCallback(struct redisClusterAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = (redisReply *) r;
    if (!r) {
        fprintf(stderr, "Redis read error 0x%x: %s\n", c->err, c->errstr);
        return;
    }

    printRedisReply((void *) ENUM_REDISREAD_BROADCAST == privdata ? "广播" : "定向通知", reply);
    if (reply->type == REDIS_REPLY_ERROR)
        return;

    const char **parsed = parseRedisReply(reply, NULL);

    if (parsed && parsed[3] && strcasecmp("demo", parsed[3]) == 0) {
        sendBroadcast(c, "\"{\\n\\t\\\"cmd\\\":\\t\\\"request/online\\\",\\n\\t\\\"sn\\\":\\t\\\"5-1600660996542\\\",\\n\\t\\\"device\\\":\\t\\\"C47F0E0A3D45\\\",\\n\\t\\\"sender\\\":\\t\\\"macbookpro\\\",\\n\\t\\\"vendor\\\":\\t\\\"KaiLu\\\",\\n\\t\\\"timestamp\\\":\\t1600660994435,\\n\\t\\\"retrans\\\":\\t0,\\n\\t\\\"data\\\":\\t{\\n\\t\\t\\\"vendor\\\":\\t\\\"\\\",\\n\\t\\t\\\"model\\\":\\t\\\"\\\",\\n\\t\\t\\\"type\\\":\\t\\\"02-联通\\\",\\n\\t\\t\\\"ccid\\\":\\t\\\"89860619050046240163\\\",\\n\\t\\t\\\"net\\\":\\t\\\"03-4G\\\",\\n\\t\\t\\\"clientip\\\":\\t\\\"127.0.0.1\\\",\\n\\t\\t\\\"clientport\\\":\\t59075\\n\\t},\\n\\t\\\"noack\\\":\\t0\\n}\"!");
    }

//    if ((void *) ENUM_REDISREAD_BROADCAST == privdata)
//        xreadStartAsync(c, "$"); //为了避免丢失数据，这里的$要修改为上次数据的时间戳
//    else
//        xdirectreadStartAsync(c, "$");
}

/**
 * 触发redis的异步读
 * @param c
 * @param timesign：读取时的时间参数，一共有3个可能：0-所有；$-当前时间之后;以及其它时间戳字符串
 */
static void __inline xreadStartAsync(redisClusterAsyncContext *c, const char *timesign) {
    int irtn = redisClusterAsyncCommand(c, xreadCallback, (void *) ENUM_REDISREAD_BROADCAST,
                                        "XREAD BLOCK 0 STREAMS %s %s", runParams.redis_streamname_read, timesign);
    if (REDIS_OK != irtn)
        fprintf(stderr, "xreadStartAsync %s\n", c->errstr);
}

static void __inline xdirectreadStartAsync(redisClusterAsyncContext *c, const char *timesign) {
    int irtn = redisClusterAsyncCommand(c, xreadCallback, (void *) ENUM_REDISREAD_DIRECT,
                                        "XADD %s/%s * data '%s'", runParams.redis_streamname_read, runParams.myname, "hehiehi");
//                                        "XREAD BLOCK 0 STREAMS %s/%s %s", runParams.redis_streamname_read, runParams.myname, timesign);
    if (REDIS_OK != irtn)
        fprintf(stderr, "xdirectreadStartAsync %s\n", c->errstr);
}

static void connectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        fprintf(stdout, "Error connect: %s\n", c->errstr);
        return;
    }
    fprintf(stdout, "Redis connected: %d\n", status);
}

static void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        fprintf(stderr, "disconnect: %s\n", c->errstr);
        return;
    }
    fprintf(stdout, "Disconnected...\n");
}

int main(const int argc, const char *asrgv[]) {
    ENUM_REDISREAD_METHOD method = ENUM_REDISREAD_BROADCAST;
    const char *caption = (ENUM_REDISREAD_BROADCAST == method) ? "redisBroadcast" : "redisDirect";

    fprintf(stdout, "Starting redis consumer %s.\n", caption);

    struct event_base *base = event_base_new();
    redisClusterAsyncContext *c = redisClusterAsyncConnect(runParams.redis_broker_list, HIRCLUSTER_FLAG_NULL);
    if (c->err) {
        fprintf(stderr, "%s\n", c->errstr);
        // handle error
    }

    redisClusterLibeventAttach(c, base);
    redisClusterAsyncSetConnectCallback(c, connectCallback);
    redisClusterAsyncSetDisconnectCallback(c, disconnectCallback);

    //由于在redis集群中无法一次性监听多个key，所以分开。报错信息为："(error) CROSSSLOT Keys in request don't hash to the same slot"
    if (ENUM_REDISREAD_BROADCAST == method)
        xreadStartAsync(c, "$");
    else
        xdirectreadStartAsync(c, "0");

    event_base_dispatch(base);

    fprintf(stdout, "Closing redis consumer %s.\n", caption);

    return 0;
}
