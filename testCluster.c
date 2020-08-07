//
// Created by liutao on 2020/7/14.
//
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "hircluster.h"
#include "adapters/libevent.h"

#include "async.h"

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

/**
 * 递归的方式打印redis的reply内容
 * @param keystr
 * @param reply
 */
static void printRedisReply(const char *title, redisReply *reply) {
    switch (reply->type) {
        case REDIS_REPLY_INTEGER:
            fprintf(stdout, "reply[%s]: %lld\n", title, reply->integer);
            break;
        case REDIS_REPLY_ARRAY: {
            char szDisplay[128] = {};
            for (int i = 0; i < reply->elements; i++) {
                snprintf(szDisplay, sizeof(szDisplay) - 1, "%s:%d", title, i + 1);
                printRedisReply(szDisplay, reply->element[i]);
            }
        }
            break;
        case REDIS_REPLY_STRING:
            fprintf(stdout, "reply[%s]: %s\n", title, reply->str);
            break;
        case REDIS_REPLY_STATUS:
            fprintf(stdout, "reply[%s]: %s\n", title, reply->str);
            break;
        case REDIS_REPLY_ERROR:
            fprintf(stdout, "reply[%s] error: %s!\n", title, reply->str);
            break;
        default:
            fprintf(stderr, "reply[%s] unsupported!\n", title);
    }
}

static void xreadCallback(struct redisClusterAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = (redisReply *) r;
    if (reply == NULL)
        return;

    printRedisReply((void *) ENUM_REDISREAD_BROADCAST == privdata ? "广播" : "定向通知", reply);
    if (reply->type == REDIS_REPLY_ERROR)
        return;

    if ((void *) ENUM_REDISREAD_BROADCAST == privdata)
        xreadStartAsync(c, "$"); //为了避免丢失数据，这里的$要修改为上次数据的时间戳
    else
        xdirectreadStartAsync(c, "$");
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
                                        "SET 1 123456789");
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
    ENUM_REDISREAD_METHOD method = ENUM_REDISREAD_DIRECT;
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
