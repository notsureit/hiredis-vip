#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <mhash.h>
#include "command.h"
#include "hiutil.h"
#include "hiarray.h"


static uint64_t cmd_id = 0;          /* command id counter */

static const struct {
    const cmd_type_t cmdType;
    const int cmdSize;
    const char *cmdCaption;
    const unsigned noforward;
    const unsigned quit;
} supportedRedisCommands[] = {
        {CMD_REQ_REDIS_XADD, 4, "xadd"},
        {CMD_REQ_REDIS_XACK, 4, "xack"},
        {CMD_REQ_REDIS_XREAD, 5, "xread"},
        {CMD_REQ_REDIS_XREADGROUP, 10, "xreadgroup"},
        {CMD_REQ_REDIS_GET, 3, "get"},
        {CMD_REQ_REDIS_SET, 3, "set"},
        {CMD_REQ_REDIS_TTL, 3, "ttl"},
        {CMD_REQ_REDIS_DEL, 3, "del"},
        {CMD_REQ_REDIS_PTTL, 4, "pttl"},
        {CMD_REQ_REDIS_DECR, 4, "decr"},
        {CMD_REQ_REDIS_DUMP, 4, "dump"},
        {CMD_REQ_REDIS_HDEL, 4, "hdel"},
        {CMD_REQ_REDIS_HGET, 4, "hget"},
        {CMD_REQ_REDIS_HLEN, 4, "hlen"},
        {CMD_REQ_REDIS_HSET, 4, "hset"},
        {CMD_REQ_REDIS_INCR, 4, "incr"},
        {CMD_REQ_REDIS_LLEN, 4, "llen"},
        {CMD_REQ_REDIS_LPOP, 4, "lpop"},
        {CMD_REQ_REDIS_LREM, 4, "lrem"},
        {CMD_REQ_REDIS_LSET, 4, "lset"},
        {CMD_REQ_REDIS_RPOP, 4, "rpop"},
        {CMD_REQ_REDIS_SADD, 4, "sadd"},
        {CMD_REQ_REDIS_SPOP, 4, "spop"},
        {CMD_REQ_REDIS_SREM, 4, "srem"},
        {CMD_REQ_REDIS_TYPE, 4, "type"},
        {CMD_REQ_REDIS_MGET, 4, "mget"},
        {CMD_REQ_REDIS_MSET, 4, "mset"},
        {CMD_REQ_REDIS_ZADD, 4, "zadd"},
        {CMD_REQ_REDIS_ZREM, 4, "zrem"},
        {CMD_REQ_REDIS_EVAL, 4, "eval"},
        {CMD_REQ_REDIS_SORT, 4, "sort"},
        {CMD_REQ_REDIS_PING, 4, "ping", 1},
        {CMD_REQ_REDIS_QUIT, 4, "quit", 0, 1},
        {CMD_REQ_REDIS_AUTH, 4, "auth", 1},
        {CMD_REQ_REDIS_HKEYS, 5, "hkeys"},
        {CMD_REQ_REDIS_HMGET, 5, "hmget"},
        {CMD_REQ_REDIS_HMSET, 5, "hmset"},
        {CMD_REQ_REDIS_HVALS, 5, "hvals"},
        {CMD_REQ_REDIS_HSCAN, 5, "hscan"},
        {CMD_REQ_REDIS_LPUSH, 5, "lpush"},
        {CMD_REQ_REDIS_LTRIM, 5, "ltrim"},
        {CMD_REQ_REDIS_RPUSH, 5, "rpush"},
        {CMD_REQ_REDIS_SCARD, 5, "scard"},
        {CMD_REQ_REDIS_SDIFF, 5, "sdiff"},
        {CMD_REQ_REDIS_SETEX, 5, "setex"},
        {CMD_REQ_REDIS_SETNX, 5, "setnx"},
        {CMD_REQ_REDIS_SMOVE, 5, "smove"},
        {CMD_REQ_REDIS_SSCAN, 5, "sscan"},
        {CMD_REQ_REDIS_ZCARD, 5, "zcard"},
        {CMD_REQ_REDIS_ZRANK, 5, "zrank"},
        {CMD_REQ_REDIS_ZSCAN, 5, "zscan"},
        {CMD_REQ_REDIS_PFADD, 5, "pfadd"},
        {CMD_REQ_REDIS_APPEND, 6, "append"},
        {CMD_REQ_REDIS_DECRBY, 6, "decrby"},
        {CMD_REQ_REDIS_EXISTS, 6, "exists"},
        {CMD_REQ_REDIS_EXPIRE, 6, "expire"},
        {CMD_REQ_REDIS_GETBIT, 6, "getbit"},
        {CMD_REQ_REDIS_GETSET, 6, "getset"},
        {CMD_REQ_REDIS_PSETEX, 6, "psetex"},
        {CMD_REQ_REDIS_HSETNX, 6, "hsetnx"},
        {CMD_REQ_REDIS_INCRBY, 6, "incrby"},
        {CMD_REQ_REDIS_LINDEX, 6, "lindex"},
        {CMD_REQ_REDIS_LPUSHX, 6, "lpushx"},
        {CMD_REQ_REDIS_LRANGE, 6, "lrange"},
        {CMD_REQ_REDIS_RPUSHX, 6, "rpushx"},
        {CMD_REQ_REDIS_SETBIT, 6, "setbit"},
        {CMD_REQ_REDIS_SINTER, 6, "sinter"},
        {CMD_REQ_REDIS_STRLEN, 6, "strlen"},
        {CMD_REQ_REDIS_SUNION, 6, "sunion"},
        {CMD_REQ_REDIS_ZCOUNT, 6, "zcount"},
        {CMD_REQ_REDIS_ZRANGE, 6, "zrange"},
        {CMD_REQ_REDIS_ZSCORE, 6, "zscore"},
        {CMD_REQ_REDIS_PERSIST, 7, "persist"},
        {CMD_REQ_REDIS_PEXPIRE, 7, "pexpire"},
        {CMD_REQ_REDIS_HEXISTS, 7, "hexists"},
        {CMD_REQ_REDIS_HGETALL, 7, "hgetall"},
        {CMD_REQ_REDIS_HINCRBY, 7, "hincrby"},
        {CMD_REQ_REDIS_LINSERT, 7, "linsert"},
        {CMD_REQ_REDIS_ZINCRBY, 7, "zincrby"},
        {CMD_REQ_REDIS_EVALSHA, 7, "evalsha"},
        {CMD_REQ_REDIS_RESTORE, 7, "restore"},
        {CMD_REQ_REDIS_PFCOUNT, 7, "pfcount"},
        {CMD_REQ_REDIS_PFMERGE, 7, "pfmerge"},
        {CMD_REQ_REDIS_EXPIREAT, 8, "expireat"},
        {CMD_REQ_REDIS_BITCOUNT, 8, "bitcount"},
        {CMD_REQ_REDIS_GETRANGE, 8, "getrange"},
        {CMD_REQ_REDIS_SETRANGE, 8, "setrange"},
        {CMD_REQ_REDIS_SMEMBERS, 8, "smembers"},
        {CMD_REQ_REDIS_ZREVRANK, 8, "zrevrank"},
        {CMD_REQ_REDIS_PEXPIREAT, 9, "pexpireat"},
        {CMD_REQ_REDIS_RPOPLPUSH, 9, "rpoplpush"},
        {CMD_REQ_REDIS_SISMEMBER, 9, "sismember"},
        {CMD_REQ_REDIS_ZREVRANGE, 9, "zrevrange"},
        {CMD_REQ_REDIS_ZLEXCOUNT, 9, "zlexcount"},
        {CMD_REQ_REDIS_SDIFFSTORE, 10, "sdiffstore"},
        {CMD_REQ_REDIS_INCRBYFLOAT, 11, "incrbyfloat"},
        {CMD_REQ_REDIS_SINTERSTORE, 11, "sinterstore"},
        {CMD_REQ_REDIS_SRANDMEMBER, 11, "srandmember"},
        {CMD_REQ_REDIS_SUNIONSTORE, 11, "sunionstore"},
        {CMD_REQ_REDIS_ZINTERSTORE, 11, "zinterstore"},
        {CMD_REQ_REDIS_ZUNIONSTORE, 11, "zunionstore"},
        {CMD_REQ_REDIS_ZRANGEBYLEX, 11, "zrangebylex"},
        {CMD_REQ_REDIS_HINCRBYFLOAT, 12, "hincrbyfloat"},
        {CMD_REQ_REDIS_ZRANGEBYSCORE, 13, "zrangebyscore"},
        {CMD_REQ_REDIS_ZREMRANGEBYLEX, 14, "zremrangebylex"},
        {CMD_REQ_REDIS_ZREMRANGEBYRANK, 15, "zremrangebyrank"},
        {CMD_REQ_REDIS_ZREMRANGEBYSCORE, 16, "zremrangebyscore"},
        {CMD_REQ_REDIS_ZREVRANGEBYSCORE, 16, "zrevrangebyscore"},
        {CMD_UNKNOWN, 0, "Unsupported!"}
};

/*
 * Return true, if the redis command take no key, otherwise
 * return false
 */
static int
redis_argz(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_PING:
    case CMD_REQ_REDIS_QUIT:
        return 1;

    default:
        break;
    }

    return 0;
}

/*
 * Return true, if the redis command accepts no arguments, otherwise
 * return false
 */
static int
redis_arg0(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_EXISTS:
    case CMD_REQ_REDIS_PERSIST:
    case CMD_REQ_REDIS_PTTL:
    case CMD_REQ_REDIS_SORT:
    case CMD_REQ_REDIS_TTL:
    case CMD_REQ_REDIS_TYPE:
    case CMD_REQ_REDIS_DUMP:

    case CMD_REQ_REDIS_DECR:
    case CMD_REQ_REDIS_GET:
    case CMD_REQ_REDIS_INCR:
    case CMD_REQ_REDIS_STRLEN:

    case CMD_REQ_REDIS_HGETALL:
    case CMD_REQ_REDIS_HKEYS:
    case CMD_REQ_REDIS_HLEN:
    case CMD_REQ_REDIS_HVALS:

    case CMD_REQ_REDIS_LLEN:
    case CMD_REQ_REDIS_LPOP:
    case CMD_REQ_REDIS_RPOP:

    case CMD_REQ_REDIS_SCARD:
    case CMD_REQ_REDIS_SMEMBERS:
    case CMD_REQ_REDIS_SPOP:

    case CMD_REQ_REDIS_ZCARD:
    case CMD_REQ_REDIS_PFCOUNT:
    case CMD_REQ_REDIS_AUTH:
        return 1;

    default:
        break;
    }

    return 0;
}

/*
 * Return true, if the redis command accepts exactly 1 argument, otherwise
 * return false
 */
static int
redis_arg1(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_EXPIRE:
    case CMD_REQ_REDIS_EXPIREAT:
    case CMD_REQ_REDIS_PEXPIRE:
    case CMD_REQ_REDIS_PEXPIREAT:

    case CMD_REQ_REDIS_APPEND:
    case CMD_REQ_REDIS_DECRBY:
    case CMD_REQ_REDIS_GETBIT:
    case CMD_REQ_REDIS_GETSET:
    case CMD_REQ_REDIS_INCRBY:
    case CMD_REQ_REDIS_INCRBYFLOAT:
    case CMD_REQ_REDIS_SETNX:

    case CMD_REQ_REDIS_HEXISTS:
    case CMD_REQ_REDIS_HGET:

    case CMD_REQ_REDIS_LINDEX:
    case CMD_REQ_REDIS_LPUSHX:
    case CMD_REQ_REDIS_RPOPLPUSH:
    case CMD_REQ_REDIS_RPUSHX:

    case CMD_REQ_REDIS_SISMEMBER:

    case CMD_REQ_REDIS_ZRANK:
    case CMD_REQ_REDIS_ZREVRANK:
    case CMD_REQ_REDIS_ZSCORE:
        return 1;

    default:
        break;
    }

    return 0;
}

/*
 * Return true, if the redis command accepts exactly 2 arguments, otherwise
 * return false
 */
static int
redis_arg2(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_GETRANGE:
    case CMD_REQ_REDIS_PSETEX:
    case CMD_REQ_REDIS_SETBIT:
    case CMD_REQ_REDIS_SETEX:
    case CMD_REQ_REDIS_SETRANGE:

    case CMD_REQ_REDIS_HINCRBY:
    case CMD_REQ_REDIS_HINCRBYFLOAT:
    case CMD_REQ_REDIS_HSET:
    case CMD_REQ_REDIS_HSETNX:

    case CMD_REQ_REDIS_LRANGE:
    case CMD_REQ_REDIS_LREM:
    case CMD_REQ_REDIS_LSET:
    case CMD_REQ_REDIS_LTRIM:

    case CMD_REQ_REDIS_SMOVE:

    case CMD_REQ_REDIS_ZCOUNT:
    case CMD_REQ_REDIS_ZLEXCOUNT:
    case CMD_REQ_REDIS_ZINCRBY:
    case CMD_REQ_REDIS_ZREMRANGEBYLEX:
    case CMD_REQ_REDIS_ZREMRANGEBYRANK:
    case CMD_REQ_REDIS_ZREMRANGEBYSCORE:

    case CMD_REQ_REDIS_RESTORE:
        return 1;

    default:
        break;
    }

    return 0;
}

/*
 * Return true, if the redis command accepts exactly 3 arguments, otherwise
 * return false
 */
static int
redis_arg3(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_LINSERT:
        return 1;

    default:
        break;
    }

    return 0;
}

/*
 * Return true, if the redis command accepts 0 or more arguments, otherwise
 * return false
 */
static int
redis_argn(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_BITCOUNT:

    case CMD_REQ_REDIS_SET:
    case CMD_REQ_REDIS_HDEL:
    case CMD_REQ_REDIS_HMGET:
    case CMD_REQ_REDIS_HMSET:
    case CMD_REQ_REDIS_HSCAN:

    case CMD_REQ_REDIS_LPUSH:
    case CMD_REQ_REDIS_RPUSH:

    case CMD_REQ_REDIS_SADD:
    case CMD_REQ_REDIS_SDIFF:
    case CMD_REQ_REDIS_SDIFFSTORE:
    case CMD_REQ_REDIS_SINTER:
    case CMD_REQ_REDIS_SINTERSTORE:
    case CMD_REQ_REDIS_SREM:
    case CMD_REQ_REDIS_SUNION:
    case CMD_REQ_REDIS_SUNIONSTORE:
    case CMD_REQ_REDIS_SRANDMEMBER:
    case CMD_REQ_REDIS_SSCAN:

    case CMD_REQ_REDIS_PFADD:
    case CMD_REQ_REDIS_PFMERGE:

    case CMD_REQ_REDIS_ZADD:
    case CMD_REQ_REDIS_ZINTERSTORE:
    case CMD_REQ_REDIS_ZRANGE:
    case CMD_REQ_REDIS_ZRANGEBYSCORE:
    case CMD_REQ_REDIS_ZREM:
    case CMD_REQ_REDIS_ZREVRANGE:
    case CMD_REQ_REDIS_ZRANGEBYLEX:
    case CMD_REQ_REDIS_ZREVRANGEBYSCORE:
    case CMD_REQ_REDIS_ZUNIONSTORE:
    case CMD_REQ_REDIS_ZSCAN:
        return 1;

    default:
        break;
    }

    return 0;
}

/*
 * Return true, if the redis command is a vector command accepting one or
 * more keys, otherwise return false
 */
static int
redis_argx(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_MGET:
    case CMD_REQ_REDIS_DEL:
        return 1;

    default:
        break;
    }

    return 0;
}

/*
 * Return true, if the redis command is a vector command accepting one or
 * more key-value pairs, otherwise return false
 */
static int
redis_argkvx(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_MSET:
        return 1;

    default:
        break;
    }

    return 0;
}

/*
 * Return true, if the redis command is either EVAL or EVALSHA. These commands
 * have a special format with exactly 2 arguments, followed by one or more keys,
 * followed by zero or more arguments (the documentation online seems to suggest
 * that at least one argument is required, but that shouldn't be the case).
 */
static int
redis_argeval(struct cmd *r)
{
    switch (r->type) {
    case CMD_REQ_REDIS_EVAL:
    case CMD_REQ_REDIS_EVALSHA:
        return 1;

    default:
        break;
    }

    return 0;
}

static int
redis_argstream(struct cmd *r)
{
    switch (r->type) {
        case CMD_REQ_REDIS_XADD:
        case CMD_REQ_REDIS_XACK:
        case CMD_REQ_REDIS_XREAD:
        case CMD_REQ_REDIS_XREADGROUP:
            return 1;

        default:
            break;
    }

    return 0;
}

static void judgeCommandType(const int len, const char *m, struct cmd *r) {
    for (int i = 0; i < sizeof(supportedRedisCommands) / sizeof(supportedRedisCommands[0]); i++) {
        if (supportedRedisCommands[i].cmdSize == len) {
            if (strncasecmp(supportedRedisCommands[i].cmdCaption, m, len) == 0) {
                r->type = supportedRedisCommands[i].cmdType;
                if (supportedRedisCommands[i].noforward)
                    r->noforward = supportedRedisCommands[i].noforward;
                if (supportedRedisCommands[i].quit)
                    r->noforward = supportedRedisCommands[i].quit;
                return;
            }
        }
    }
}

static const char *getCommandCaptionByType(cmd_type_t type)
{
    for (int i = 0; i < sizeof(supportedRedisCommands) / sizeof(supportedRedisCommands[0]); i++) {
        if (supportedRedisCommands[i].cmdType == type) {
            return supportedRedisCommands[i].cmdCaption;
        }
    }
    return supportedRedisCommands[sizeof(supportedRedisCommands) / sizeof(supportedRedisCommands[0]) - 1].cmdCaption;
}

/*
 * Reference: http://redis.io/topics/protocol
 *
 * Redis >= 1.2 uses the unified protocol to send requests to the Redis
 * server. In the unified protocol all the arguments sent to the server
 * are binary safe and every request has the following general form:
 *
 *   *<number of arguments> CR LF
 *   $<number of bytes of argument 1> CR LF
 *   <argument data> CR LF
 *   ...
 *   $<number of bytes of argument N> CR LF
 *   <argument data> CR LF
 *
 * Before the unified request protocol, redis protocol for requests supported
 * the following commands
 * 1). Inline commands: simple commands where arguments are just space
 *     separated strings. No binary safeness is possible.
 * 2). Bulk commands: bulk commands are exactly like inline commands, but
 *     the last argument is handled in a special way in order to allow for
 *     a binary-safe last argument.
 *
 * only supports the Redis unified protocol for requests.
 */
void
redis_parse_cmd(struct cmd *r)
{
    int len;
    char *p, *m, *token = NULL;
    char *cmd_end;
    char ch;
    uint32_t rlen = 0;  /* running length in parsing fsa */
    uint32_t rnarg = 0; /* running # arg used by parsing fsa */
    char lastArgs[128] = {0}; //保存上次的参数

    enum {
        SW_START,
        SW_NARG,
        SW_NARG_LF,
        SW_REQ_TYPE_LEN,
        SW_REQ_TYPE_LEN_LF,
        SW_REQ_TYPE,
        SW_REQ_TYPE_LF,
        SW_KEY_LEN,
        SW_KEY_LEN_LF,
        SW_KEY,
        SW_KEY_LF,
        SW_ARG1_LEN,
        SW_ARG1_LEN_LF,
        SW_ARG1,
        SW_ARG1_LF,
        SW_ARG2_LEN,
        SW_ARG2_LEN_LF,
        SW_ARG2,
        SW_ARG2_LF,
        SW_ARG3_LEN,
        SW_ARG3_LEN_LF,
        SW_ARG3,
        SW_ARG3_LF,
        SW_ARGN_LEN,
        SW_ARGN_LEN_LF,
        SW_ARGN,
        SW_ARGN_LF,
        SW_SENTINEL
    } state;

    state = SW_START;
    cmd_end = r->cmd + r->clen;

    ASSERT(state >= SW_START && state < SW_SENTINEL);
    ASSERT(r->cmd != NULL && r->clen > 0);

    for (p = r->cmd; p < cmd_end; p++) {
        ch = *p;

        switch (state) {

        case SW_START:
        case SW_NARG:
            if (token == NULL) {
                if (ch != '*') {
                    goto error;
                }
                token = p;
                /* req_start <- p */
                r->narg_start = p;
                rnarg = 0;
                state = SW_NARG;
            } else if (isdigit(ch)) {
                rnarg = rnarg * 10 + (uint32_t)(ch - '0');
            } else if (ch == CR) {
                if (rnarg == 0) {
                    goto error;
                }
                r->narg = rnarg;
                r->narg_end = p;
                token = NULL;
                state = SW_NARG_LF;
            } else {
                goto error;
            }

            break;

        case SW_NARG_LF:
            switch (ch) {
            case LF:
                state = SW_REQ_TYPE_LEN;
                break;

            default:
                goto error;
            }

            break;

        case SW_REQ_TYPE_LEN:
            if (token == NULL) {
                if (ch != '$') {
                    goto error;
                }
                token = p;
                rlen = 0;
            } else if (isdigit(ch)) {
                rlen = rlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == CR) {
                if (rlen == 0 || rnarg == 0) {
                    goto error;
                }
                rnarg--;
                token = NULL;
                state = SW_REQ_TYPE_LEN_LF;
            } else {
                goto error;
            }

            break;

        case SW_REQ_TYPE_LEN_LF:
            switch (ch) {
            case LF:
                state = SW_REQ_TYPE;
                break;

            default:
                goto error;
            }

            break;

        case SW_REQ_TYPE:
            if (token == NULL) {
                token = p;
            }

            m = token + rlen;
            if (m >= cmd_end) {
                //m = cmd_end - 1;
                //p = m;
                //break;
                goto error;
            }

            if (*m != CR) {
                goto error;
            }

            p = m; /* move forward by rlen bytes */
            rlen = 0;
            m = token;
            token = NULL;
            r->type = CMD_UNKNOWN;

            judgeCommandType(p - m, m, r);

            if (r->type == CMD_UNKNOWN) {
                goto error;
            }

            state = SW_REQ_TYPE_LF;
            break;

        case SW_REQ_TYPE_LF:
            switch (ch) {
            case LF:
                if (redis_argz(r)) {
                    goto done;
                } else if (redis_argeval(r)) {
                    state = SW_ARG1_LEN;
                } else if (redis_argstream(r)) {
                    state = SW_ARG1_LEN;
                } else {
                    state = SW_KEY_LEN;
                }
                break;

            default:
                goto error;
            }

            break;

        case SW_KEY_LEN:
            if (token == NULL) {
                if (ch != '$') {
                    goto error;
                }
                token = p;
                rlen = 0;
            } else if (isdigit(ch)) {
                rlen = rlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == CR) {
                
                if (rnarg == 0) {
                    goto error;
                }
                rnarg--;
                token = NULL;
                state = SW_KEY_LEN_LF;
            } else {
                goto error;
            }

            break;

        case SW_KEY_LEN_LF:
            switch (ch) {
            case LF:
                state = SW_KEY;
                break;

            default:
                goto error;
            }

            break;

        case SW_KEY:
            if (token == NULL) {
                token = p;
            }

            m = token + rlen;
            if (m >= cmd_end) {
                //m = b->last - 1;
                //p = m;
                //break;
                goto error;
            }

            if (*m != CR) {
                goto error;
            } else {        /* got a key */
                struct keypos *kpos;

                p = m;      /* move forward by rlen bytes */
                rlen = 0;
                m = token;
                token = NULL;

                kpos = hiarray_push(r->keys);
                if (kpos == NULL) {
                    goto enomem;
                }
                kpos->start = m;
                kpos->end = p;
                //kpos->v_len = 0;

                state = SW_KEY_LF;
            }

            break;

        case SW_KEY_LF:
            switch (ch) {
            case LF:
                if (redis_arg0(r)) {
                    if (rnarg != 0) {
                        goto error;
                    }
                    goto done;
                } else if (redis_arg1(r)) {
                    if (rnarg != 1) {
                        goto error;
                    }
                    state = SW_ARG1_LEN;
                } else if (redis_arg2(r)) {
                    if (rnarg != 2) {
                        goto error;
                    }
                    state = SW_ARG1_LEN;
                } else if (redis_arg3(r)) {
                    if (rnarg != 3) {
                        goto error;
                    }
                    state = SW_ARG1_LEN;
                } else if (redis_argn(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARG1_LEN;
                } else if (redis_argx(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_KEY_LEN;
                } else if (redis_argkvx(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    if (r->narg % 2 == 0) {
                        goto error;
                    }
                    state = SW_ARG1_LEN;
                } else if (redis_argeval(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARGN_LEN;
                } else if (redis_argstream(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARGN_LEN;
                } else {
                    goto error;
                }

                break;

            default:
                goto error;
            }

            break;

        case SW_ARG1_LEN:
            if (token == NULL) {
                if (ch != '$') {
                    goto error;
                }
                rlen = 0;
                token = p;
            } else if (isdigit(ch)) {
                rlen = rlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == CR) {
                if ((p - token) <= 1 || rnarg == 0) {
                    goto error;
                }
                rnarg--;
                token = NULL;

                /*
                //for mset value length
                if(redis_argkvx(r))
                {
                    struct keypos *kpos;
                    uint32_t array_len = array_n(r->keys);
                    if(array_len == 0)
                    {
                        goto error;
                    }
                    
                    kpos = array_n(r->keys, array_len-1);
                    if (kpos == NULL || kpos->v_len != 0) {
                        goto error;
                    }

                    kpos->v_len = rlen;
                }
                */
                state = SW_ARG1_LEN_LF;
            } else {
                goto error;
            }

            break;

        case SW_ARG1_LEN_LF:
            switch (ch) {
            case LF:
                state = SW_ARG1;
                break;

            default:
                goto error;
            }

            break;

        case SW_ARG1:
            m = p + rlen;
            if (m >= cmd_end) {
                //rlen -= (uint32_t)(b->last - p);
                //m = b->last - 1;
                //p = m;
                //break;
                goto error;
            }

            if (*m != CR) {
                goto error;
            }

            p = m; /* move forward by rlen bytes */
            rlen = 0;

            state = SW_ARG1_LF;

            break;

        case SW_ARG1_LF:
            switch (ch) {
            case LF:
                if (redis_arg1(r)) {
                    if (rnarg != 0) {
                        goto error;
                    }
                    goto done;
                } else if (redis_arg2(r)) {
                    if (rnarg != 1) {
                        goto error;
                    }
                    state = SW_ARG2_LEN;
                } else if (redis_arg3(r)) {
                    if (rnarg != 2) {
                        goto error;
                    }
                    state = SW_ARG2_LEN;
                } else if (redis_argn(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARGN_LEN;
                } else if (redis_argeval(r)) {
                    if (rnarg < 2) {
                        goto error;
                    }
                    state = SW_ARG2_LEN;
                } else if (redis_argkvx(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_KEY_LEN;
                } else if (redis_argstream(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARGN_LEN;
                } else {
                    goto error;
                }

                break;

            default:
                goto error;
            }

            break;

        case SW_ARG2_LEN:
            if (token == NULL) {
                if (ch != '$') {
                    goto error;
                }
                rlen = 0;
                token = p;
            } else if (isdigit(ch)) {
                rlen = rlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == CR) {
                if ((p - token) <= 1 || rnarg == 0) {
                    goto error;
                }
                rnarg--;
                token = NULL;
                state = SW_ARG2_LEN_LF;
            } else {
                goto error;
            }

            break;

        case SW_ARG2_LEN_LF:
            switch (ch) {
            case LF:
                state = SW_ARG2;
                break;

            default:
                goto error;
            }

            break;

        case SW_ARG2:
            if (token == NULL && redis_argeval(r)) {
                /*
                 * For EVAL/EVALSHA, ARG2 represents the # key/arg pairs which must
                 * be tokenized and stored in contiguous memory.
                 */
                token = p;
            }

            m = p + rlen;
            if (m >= cmd_end) {
                //rlen -= (uint32_t)(b->last - p);
                //m = b->last - 1;
                //p = m;
                //break;
                goto error;
            }

            if (*m != CR) {
                goto error;
            }

            p = m; /* move forward by rlen bytes */
            rlen = 0;

            if (redis_argeval(r)) {
                uint32_t nkey;
                char *chp;

                /*
                 * For EVAL/EVALSHA, we need to find the integer value of this
                 * argument. It tells us the number of keys in the script, and
                 * we need to error out if number of keys is 0. At this point,
                 * both p and m point to the end of the argument and r->token
                 * points to the start.
                 */
                if (p - token < 1) {
                    goto error;
                }

                for (nkey = 0, chp = token; chp < p; chp++) {
                    if (isdigit(*chp)) {
                        nkey = nkey * 10 + (uint32_t)(*chp - '0');
                    } else {
                        goto error;
                    }
                }
                if (nkey == 0) {
                    goto error;
                }

                token = NULL;
            }

            state = SW_ARG2_LF;

            break;

        case SW_ARG2_LF:
            switch (ch) {
            case LF:
                if (redis_arg2(r)) {
                    if (rnarg != 0) {
                        goto error;
                    }
                    goto done;
                } else if (redis_arg3(r)) {
                    if (rnarg != 1) {
                        goto error;
                    }
                    state = SW_ARG3_LEN;
                } else if (redis_argn(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARGN_LEN;
                } else if (redis_argeval(r)) {
                    if (rnarg < 1) {
                        goto error;
                    }
                    state = SW_KEY_LEN;
                } else {
                    goto error;
                }

                break;

            default:
                goto error;
            }

            break;

        case SW_ARG3_LEN:
            if (token == NULL) {
                if (ch != '$') {
                    goto error;
                }
                rlen = 0;
                token = p;
            } else if (isdigit(ch)) {
                rlen = rlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == CR) {
                if ((p - token) <= 1 || rnarg == 0) {
                    goto error;
                }
                rnarg--;
                token = NULL;
                state = SW_ARG3_LEN_LF;
            } else {
                goto error;
            }

            break;

        case SW_ARG3_LEN_LF:
            switch (ch) {
            case LF:
                state = SW_ARG3;
                break;

            default:
                goto error;
            }

            break;

        case SW_ARG3:
            m = p + rlen;
            if (m >= cmd_end) {
                //rlen -= (uint32_t)(b->last - p);
                //m = b->last - 1;
                //p = m;
                //break;
                goto error;
            }

            if (*m != CR) {
                goto error;
            }

            p = m; /* move forward by rlen bytes */
            rlen = 0;
            state = SW_ARG3_LF;

            break;

        case SW_ARG3_LF:
            switch (ch) {
            case LF:
                if (redis_arg3(r)) {
                    if (rnarg != 0) {
                        goto error;
                    }
                    goto done;
                } else if (redis_argn(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARGN_LEN;
                } else {
                    goto error;
                }

                break;

            default:
                goto error;
            }

            break;

        case SW_ARGN_LEN:
            if (token == NULL) {
                if (ch != '$') {
                    goto error;
                }
                rlen = 0;
                token = p;
            } else if (isdigit(ch)) {
                rlen = rlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == CR) {
                if ((p - token) <= 1 || rnarg == 0) {
                    goto error;
                }
                rnarg--;
                token = NULL;
                state = SW_ARGN_LEN_LF;
            } else {
                goto error;
            }

            break;

        case SW_ARGN_LEN_LF:
            switch (ch) {
            case LF:
                state = SW_ARGN;
                break;

            default:
                goto error;
            }

            break;

        case SW_ARGN:
            strncpy(lastArgs, p, rlen);
            m = p + rlen;
            if (m >= cmd_end) {
                //rlen -= (uint32_t)(b->last - p);
                //m = b->last - 1;
                //p = m;
                //break;
                goto error;
            }

            if (*m != CR) {
                goto error;
            }

            p = m; /* move forward by rlen bytes */
            rlen = 0;
            state = SW_ARGN_LF;

            break;

        case SW_ARGN_LF:
            switch (ch) {
            case LF:
                if (redis_argn(r) || redis_argeval(r)) {
                    if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARGN_LEN;
                } else if (redis_argstream(r)) {
                    if (strncasecmp(lastArgs, "STREAMS", sizeof("STREAMS") - 1) == 0) {
                        state = SW_KEY_LEN;
                        break;
                    } else if (rnarg == 0) {
                        goto done;
                    }
                    state = SW_ARGN_LEN;
                } else {
                    goto error;
                }

                break;

            default:
                goto error;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;
        }
    }

    ASSERT(p == cmd_end);

    return;

done:

    ASSERT(r->type > CMD_UNKNOWN && r->type < CMD_SENTINEL);
    
    r->result = CMD_PARSE_OK;

    return;

enomem:
    
    r->result = CMD_PARSE_ENOMEM;

    return;

error:
    
    r->result = CMD_PARSE_ERROR;
    errno = EINVAL;
    if(r->errstr == NULL){
        r->errstr = hi_alloc(100*sizeof(*r->errstr));
    }

    len = _scnprintf(r->errstr, 100,
                     "Parse command error. Cmd type: %d/%s, state: %d, break position: %d.",
                     r->type, getCommandCaptionByType(r->type), state, (int) (p - r->cmd));
    r->errstr[len] = '\0';
}

struct cmd *command_get()
{
    struct cmd *command;
    command = hi_alloc(sizeof(struct cmd));
    if(command == NULL)
    {
        return NULL;
    }
        
    command->id = ++cmd_id;
    command->result = CMD_PARSE_OK;
    command->errstr = NULL;
    command->type = CMD_UNKNOWN;
    command->cmd = NULL;
    command->clen = 0;
    command->keys = NULL;
    command->narg_start = NULL;
    command->narg_end = NULL;
    command->narg = 0;
    command->quit = 0;
    command->noforward = 0;
    command->slot_num = -1;
    command->frag_seq = NULL;
    command->reply = NULL;
    command->sub_commands = NULL;

    command->keys = hiarray_create(1, sizeof(struct keypos));
    if (command->keys == NULL) 
    {
        hi_free(command);
        return NULL;
    }

    return command;
}

void command_destroy(struct cmd *command)
{
    if(command == NULL)
    {
        return;
    }

    if(command->cmd != NULL)
    {
        free(command->cmd);
    }

    if(command->errstr != NULL){
        hi_free(command->errstr);
    }

    if(command->keys != NULL)
    {
        command->keys->nelem = 0;
        hiarray_destroy(command->keys);
    }

    if(command->frag_seq != NULL)
    {
        hi_free(command->frag_seq);
        command->frag_seq = NULL;
    }

    if(command->reply != NULL)
    {
        freeReplyObject(command->reply);
    }

    if(command->sub_commands != NULL)
    {
        listRelease(command->sub_commands);
    }
    
    hi_free(command);
}


