package com.fyc.tools;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisExhaustedPoolException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


public class StrongJedisClient {

    private static final Map<String, StrongJedisClient> simpleClientPool = new HashMap<>();
    private final String instanceThreadKey;

    public static StrongJedisClient getInstance() {
        return getInstance(null, null, null, null);
    }

    public static StrongJedisClient getInstance(Integer timeout) {
        return getInstance(null, null, timeout, null);
    }

    public static StrongJedisClient getInstance(String host, Integer port, Integer timeout, String password) {
        String instanceThreadKey = getThreadInstanceKey(host, port, timeout, password);

        if (!simpleClientPool.containsKey(instanceThreadKey)) {
            synchronized (simpleClientPool) {
                if (!simpleClientPool.containsKey(instanceThreadKey)) {
                    simpleClientPool.put(instanceThreadKey, new StrongJedisClient(host, port, timeout, password));
                }
            }
        }
        StrongJedisClient client = simpleClientPool.get(instanceThreadKey);
        client.incrRefCount();
        return client;
    }


    private static String getThreadInstanceKey(String host, Integer port, Integer timeout, String password) {
        Long curId = Thread.currentThread().getId();
        String instanceThreadKey = curId + "";
        if (host != null) {
            instanceThreadKey += "host:" + host;
        }
        if (port != null) {
            instanceThreadKey += "port" + port;
        }
        if (timeout != null) {
            instanceThreadKey += "timeout" + timeout;
        }
        if (password != null) {
            instanceThreadKey += "password" + password;
        }
        return instanceThreadKey;
    }

    private static final AtomicInteger totalBorrowCount = new AtomicInteger();
    private static final AtomicInteger totalReturnCount = new AtomicInteger();
    private static final AtomicLong maxDurTime = new AtomicLong(0L);
    private static final AtomicLong totalDurTime = new AtomicLong(0L);
    private final long allocRes;
    private long returnRes;
    private int refCount;

    private int incrRefCount() {
        refCount++;
        return refCount;
    }

    private void decrRefCount() {
        refCount--;
    }

    private static final Logger logger = LoggerFactory.getLogger(StrongJedisClient.class);
    private final JedisPool jedisPool;
    private Jedis jedis;
    /**
     * 重试次数，默认3次。
     * todo:下一次要修改为读取配置文件的。
     */
    private static final int retryTimes = 3;
    /**
     * 获取一个强化后的jedis客户端
     */
    private StrongJedisClient() {
        this(null, null, null, null);
    }
    /**
     * 获取一个强化后的jedis客户端，指定超时时间
     *
     * @param timeout 超时时间
     */
    private StrongJedisClient(Integer timeout) {
        this(null, null, timeout, null);
    }
    /**
     * 根据指定的参数来获取一个强化后的jedis客户端
     *
     * @param host     host
     * @param port     port
     * @param timeout  timeout
     * @param password password
     */
    private StrongJedisClient(String host, Integer port, Integer timeout, String password) {
        instanceThreadKey = getThreadInstanceKey(host, port, timeout, password);
        jedisPool = JedisPoolUtil.GetPool(host, port, timeout, password);
        totalBorrowCount.incrementAndGet();
        allocRes = System.nanoTime();
        refCount = 0;
    }
    /**
     * 强化jedis客户端专用的Error，发出该异常时，需要停止程序。
     */
    public class JedisOperateFatalError extends Error {
        static final long serialVersionUID = 1L;
        public JedisOperateFatalError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static String showPoolStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("MaxDurTime", maxDurTime.get());
        status.put("MeanDurTime", totalDurTime.get() / totalReturnCount.get());
        status.put("TotalBorrowCount", totalBorrowCount.get());
        status.put("TotalReturnCount", totalReturnCount.get());
        StrongJedisClient curJedisClient = StrongJedisClient.getInstance();
        status.put("MaxBorrowWaitTimeMillis", curJedisClient.jedisPool.getMaxBorrowWaitTimeMillis());
        status.put("MeanBorrowWaitTimeMillis", curJedisClient.jedisPool.getMeanBorrowWaitTimeMillis());
        status.put("NumActive", curJedisClient.jedisPool.getNumActive());
        status.put("NumIdle", curJedisClient.jedisPool.getNumIdle());
        status.put("NumWaiters", curJedisClient.jedisPool.getNumWaiters());
        return JSON.toJSONString(status);
    }
    /**
     * 归还redis资源
     */
    public void close() {
        this.decrRefCount();
        if (refCount <= 0) {
            if (jedis != null) {
                jedis.close();
                jedis = null;
                totalReturnCount.incrementAndGet();
                returnRes = System.nanoTime();
                long dur = returnRes - allocRes;
                totalDurTime.addAndGet(dur);
                if (dur > maxDurTime.get()) {
                    maxDurTime.set(dur);
                }
            }
            if (simpleClientPool.containsKey(instanceThreadKey)) {
                synchronized (simpleClientPool) {
                    if (simpleClientPool.containsKey(instanceThreadKey)) {
                        simpleClientPool.remove(instanceThreadKey);
                    }
                }
            }
        }
    }

    /**
     * Test if the specified key exists. The command returns true if the key exists, otherwise false is
     * returned. Note that even keys set with an empty string as value will return true. Time
     * complexity: O(1)
     *
     * @param key
     * @return Boolean reply, true if the key exists, otherwise false
     */
    public Boolean exists(final String key) {
        return (Boolean) doRedisCommand(COMMAND.exists, key);
    }

    /**
     * Set a timeout on the specified key. After the timeout the key will be automatically deleted by
     * the server. A key with an associated timeout is said to be volatile in Redis terminology.
     * <p>
     * Volatile keys are stored on disk like the other keys, the timeout is persistent too like all the
     * other aspects of the dataset. Saving a dataset containing expires and stopping the server does
     * not stop the flow of time as Redis stores on disk the time when the key will no longer be
     * available as Unix time, and not the remaining seconds.
     * <p>
     * Since Redis 2.1.3 you can update the value of the timeout of a key already having an expire
     * set. It is also possible to undo the expire at all turning the key into a normal key using the
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param seconds
     * @return Integer reply, specifically: 1: the timeout was set. 0: the timeout was not set since
     * the key already has an associated timeout (this may happen only in Redis versions &lt;
     * 2.1.3, Redis &gt;= 2.1.3 will happily update the timeout), or the key does not exist.
     * @see <a href="http://redis.io/commands/expire">Expire Command</a>
     */
    public Long expire(final String key, final int seconds) {
        return (Long) doRedisCommand(COMMAND.expire, key, seconds);
    }

    /**
     * Set the string value as value of the key. The string can't be longer than 1073741824 bytes (1
     * GB).
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param value
     * @return Status code reply
     */
    public String set(final String key, final String value) {
        return (String) doRedisCommand(COMMAND.set, key, value);
    }

    /**
     * Get the value of the specified key. If the key does not exist null is returned. If the value
     * stored at key is not a string an error is returned because GET can only handle string values.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @return Bulk reply
     */
    public String get(final String key) {
        return (String) doRedisCommand(COMMAND.get, key);
    }

    /**
     * Remove the specified keys. If a given key does not exist no operation is performed for this
     * key. The command returns the number of keys removed. Time complexity: O(1)
     *
     * @param keys
     * @return Integer reply, specifically: an integer greater than 0 if one or more keys were removed
     * 0 if none of the specified key existed
     */
    public Long del(final String... keys) {
        return (Long) doRedisCommand(COMMAND.del, null, keys);
    }

    public Long del(final String key) {
        return (Long) doRedisCommand(COMMAND.del, key);
    }

    /**
     * Increment the number stored at key by one. If the key does not exist or contains a value of a
     * wrong type, set the key to the value of "0" before to perform the increment operation.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
     * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
     * and then converted back as a string.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @return Integer reply, this commands will reply with the new value of key after the increment.
     * @see #incrBy(String, long)
     */
    public Long incr(final String key) {
        return (Long) doRedisCommand(COMMAND.incr, key);
    }

    /**
     * INCRBY work just like {@link #incr(String) INCR} but instead to increment by 1 the increment is
     * integer.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are not "integer" types.
     * Simply the string stored at the key is parsed as a base 10 64 bit signed integer, incremented,
     * and then converted back as a string.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param increment
     * @return Integer reply, this commands will reply with the new value of key after the increment.
     * @see #incr(String)
     */
    public Long incrBy(final String key, final long increment) {
        return (Long) doRedisCommand(COMMAND.incrBy, key, increment);
    }

    /**
     * Set the specified hash field to the specified value.
     * <p>
     * If key does not exist, a new key holding a hash is created.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param field
     * @param value
     * @return If the field already exists, and the HSET just produced an update of the value, 0 is
     * returned, otherwise if a new field is created 1 is returned.
     */
    public Long hset(String key, String field, String value) {
        return (Long) doRedisCommand(COMMAND.hset, key, field, value);
    }

    /**
     * If key holds a hash, retrieve the value associated to the specified field.
     * <p>
     * If the field is not found or the key does not exist, a special 'nil' value is returned.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param field
     * @return Bulk reply
     */
    public String hget(final String key, final String field) {
        return (String) doRedisCommand(COMMAND.hget, key, field);
    }

    /**
     * Return the number of items in a hash.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @return The number of entries (fields) contained in the hash stored at key. If the specified
     * key does not exist, 0 is returned assuming an empty hash.
     */
    public Long hlen(final String key) {
        return (Long) doRedisCommand(COMMAND.hlen, key);
    }

    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return this.hscan(key, cursor, new ScanParams());
    }

    /**
     * Remove the specified field from an hash stored at key.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param fields
     * @return If the field was present in the hash it is deleted and 1 is returned, otherwise 0 is
     * returned and no operation is performed.
     */
    public Long hdel(final String key, final String... fields) {
        return (Long) doRedisCommand(COMMAND.hdel, key, fields);
    }


    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
        return (ScanResult<Map.Entry<String, String>>) doRedisCommand(COMMAND.hscan, key, cursor, params);
    }

    /**
     * Return all the fields and associated values in a hash.
     * <p>
     * <b>Time complexity:</b> O(N), where N is the total number of entries
     *
     * @param key
     * @return All the fields and values contained into a hash.
     */
    public Map<String, String> hgetAll(final String key) {
        return (Map<String, String>) doRedisCommand(COMMAND.hgetAll, key);
    }

    /**
     * Retrieve the values associated to the specified fields.
     * <p>
     * If some of the specified fields do not exist, nil values are returned. Non existing keys are
     * considered like empty hashes.
     * <p>
     * <b>Time complexity:</b> O(N) (with N being the number of fields)
     *
     * @param key
     * @param fields
     * @return Multi Bulk Reply specifically a list of all the values associated with the specified
     * fields, in the same order of the request.
     */
    public List<String> hmget(final String key, final String... fields) {
        return (List<String>) doRedisCommand(COMMAND.hmget, key, fields);
    }

    /**
     * Set the respective fields to the respective values. HMSET replaces old values with new values.
     * <p>
     * If key does not exist, a new key holding a hash is created.
     * <p>
     * <b>Time complexity:</b> O(N) (with N being the number of fields)
     *
     * @param key
     * @param hash
     * @return Return OK or Exception if hash is empty
     */
    public String hmset(final String key, final Map<String, String> hash) {
        return (String) doRedisCommand(COMMAND.hmset, key, hash);
    }

    /**
     * Return all the values in a hash.
     * <p>
     * <b>Time complexity:</b> O(N), where N is the total number of entries
     *
     * @param key
     * @return All the fields values contained into a hash.
     */
    public List<String> hvals(final String key) {
        return (List<String>) doRedisCommand(COMMAND.hvals, key);
    }

    /**
     * Increment the number stored at field in the hash at key by value. If key does not exist, a new
     * key holding a hash is created. If field does not exist or holds a string, the value is set to 0
     * before applying the operation. Since the value argument is signed you can use this command to
     * perform both increments and decrements.
     * <p>
     * The range of values supported by HINCRBY is limited to 64 bit signed integers.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param field
     * @param value
     * @return Integer reply The new value at field after the increment operation.
     */
    public Long hincrBy(final String key, final String field, final long value) {
        return (Long) doRedisCommand(COMMAND.hincrBy, key, field, value);
    }

    /**
     * Test for existence of a specified field in a hash. <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param field
     * @return Return true if the hash stored at key contains the specified field. Return false if the key is
     * not found or the field is not present.
     */
    public Boolean hexists(final String key, final String field) {
        return (Boolean) doRedisCommand(COMMAND.hexists, key, field);
    }

    /**
     * Add the specified member to the set value stored at key. If member is already a member of the
     * set no operation is performed. If key does not exist a new set with the specified member as
     * sole member is created. If the key exists but does not hold a set value an error is returned.
     * <p>
     * Time complexity O(1)
     *
     * @param key
     * @param members
     * @return Integer reply, specifically: 1 if the new element was added 0 if the element was
     * already a member of the set
     */
    public Long sadd(final String key, final String... members) {
        return (Long) doRedisCommand(COMMAND.sadd, key, members);
    }

    /**
     * 获取资源，超过3次就会报错
     */
    private void getResource() {
        getResource(0);
    }

    /**
     * 获取资源，超过3次就会报错
     *
     * @param retried 可以设定已经试了几次
     */
    private void getResource(int retried) {
        int reConn = retried;
        Throwable lastError;
        do {
            try {
                if (this.jedis == null) {
                    this.jedis = jedisPool.getResource();
                }
                return;
            } catch (JedisConnectionException connectionException) {
                logger.warn("发生jedis连接异常，即将重新连接，已尝试次数：" + reConn + ". " + connectionException.getMessage());
                if (jedis != null) {
                    jedis.close();
                    jedis = null;
                }
                reConn++;
                lastError = connectionException;
            } catch (JedisExhaustedPoolException exception) {
                logger.warn("资源池已满，即将重新连接，已尝试次数：" + reConn + ". " + exception.getMessage());
                if (jedis != null) {
                    jedis.close();
                    jedis = null;
                }
                reConn++;
                lastError = exception;
                logger.warn(showPoolStatus());
            }
        }
        while (reConn < retryTimes);
        logger.error("发生jedis连接异常，尝试次数：" + reConn + ". 抛出异常：" + lastError.getMessage());
        throw new JedisOperateFatalError("重试获取redis资源" + reConn + "次失败。", lastError);
    }

    /**
     * 执行redis命令。
     * 首先获取jedis资源，如果3次失败，就会抛出异常。
     * 然后执行命令，如果是dataException，就会重试执行。3次后会抛出异常。
     * 如果是ConnectionException，就会重新获取jedis资源.3次后会抛出异常。
     *
     * @param command redis命令
     * @param params  命令所需的参数
     * @return 根据不同的命令返回不同的结果
     */
    private Object doRedisCommand(String command, String key, Object... params) {
        getResource();
        int retry = 0;
        Throwable lastError;
        do {
            try {
                // do command here
                switch (command) {
                    case COMMAND.set:
                        return set(jedis, key, (String) params[0]);
                    case COMMAND.get:
                        return get(jedis, key);
                    case COMMAND.del:
                        if (key == null) {
                            return del(jedis, (String[]) params);
                        } else {
                            return del(jedis, key);
                        }
                    case COMMAND.incr:
                        return incr(jedis, key);
                    case COMMAND.incrBy:
                        return incrBy(jedis, key, (long) params[0]);
                    case COMMAND.hget:
                        return hget(jedis, key, (String) params[0]);
                    case COMMAND.hset:
                        return hset(jedis, key, (String) params[0], (String) params[1]);
                    case COMMAND.hlen:
                        return hlen(jedis, key);
                    case COMMAND.hdel:
                        return hdel(jedis, key, (String[]) params);
                    case COMMAND.hgetAll:
                        return hgetAll(jedis, key);
                    case COMMAND.hmget:
                        return hmget(jedis, key, (String[]) params);
                    case COMMAND.hmset:
                        return hmset(jedis, key, (Map<String, String>) params[0]);
                    case COMMAND.hvals:
                        return hvals(jedis, key);
                    case COMMAND.hscan:
                        return hscan(jedis, key, (String) params[0], (ScanParams) params[1]);
                    case COMMAND.hincrBy:
                        return hincrBy(jedis, key, (String) params[0], (Long) params[1]);
                    case COMMAND.hexists:
                        return hexists(jedis, key, (String) params[0]);
                    case COMMAND.sadd:
                        return sadd(jedis, key, (String[]) params);
                    case COMMAND.exists:
                        return exists(jedis, key);
                    case COMMAND.expire:
                        return expire(jedis, key, (Integer) params[0]);
                    default:
                        throw new NoSuchMethodException("没有这个方法：" + command);
                }
            } catch (JedisDataException dataException) {
                logger.warn("发生jedis数据异常，即将重新操作，已尝试次数：" + retry + ". " + dataException.getMessage());
                // 执行重试
                lastError = dataException;
                retry++;
            } catch (JedisConnectionException connectionException) {
                // 执行重连和重试
                if (jedis != null) {
                    jedis.close();
                    jedis = null;
                }
                getResource(retry);
                lastError = connectionException;
                retry++;
            } catch (NoSuchMethodException noSuchMethodException) {
                throw new RuntimeException("内部错误：", noSuchMethodException);
            } catch (Exception ex) {
                // 其他类型的异常，输出线程池状态看一下
                logger.warn(showPoolStatus());
                throw ex;
            }
        }
        while (retry < retryTimes);
        List<String> strParams = Arrays.stream(params).map(Object::toString).collect(Collectors.toList());
        logger.error("发生jedis数据异常，尝试次数：" + retry + ". 抛出异常" + lastError.getMessage(), "command:" + command
                + "params:" + String.join(",", strParams));
        throw new JedisOperateFatalError("command:" + command
                + "params:" + String.join(",", strParams), lastError);
    }

    /**
     * 真正的执行jedis hset
     *
     * @param jedis
     * @param key
     * @param field
     * @param value
     * @return
     */
    private Long hset(Jedis jedis, String key, String field, String value) {
        return jedis.hset(key, field, value);
    }

    static class COMMAND {
        public final static String set = "set";
        public final static String get = "get";
        public final static String del = "del";
        public final static String hget = "hget";
        public final static String hset = "hset";
        public final static String hlen = "hlen";
        public final static String hgetAll = "hgetAll";
        public final static String hmget = "hmget";
        public final static String hmset = "hmset";
        public final static String hvals = "hvals";
        public final static String hscan = "hscan";
        public final static String hincrBy = "hincrBy";
        public final static String hexists = "hexists";
        public final static String incrBy = "incrBy";
        public final static String incr = "incr";
        public final static String hdel = "hdel";
        public final static String sadd = "sadd";
        public final static String exists = "exists";
        public final static String expire = "expire";
    }

    private String set(Jedis jedis, String key, String value) {
        return jedis.set(key, value);
    }

    private String get(Jedis jedis, String key) {
        return jedis.get(key);
    }

    private Long del(Jedis jedis, final String... keys) {
        return jedis.del(keys);
    }

    private Long del(Jedis jedis, final String key) {
        return jedis.del(key);
    }

    /**
     * 真正的执行jedis hlen
     *
     * @param jedis
     * @param key
     * @return
     */
    private Long hlen(Jedis jedis, final String key) {
        return jedis.hlen(key);
    }

    private Map<String, String> hgetAll(Jedis jedis, final String key) {
        return jedis.hgetAll(key);
    }

    private List<String> hmget(Jedis jedis, final String key, final String... fields) {
        return jedis.hmget(key, fields);
    }

    private String hmset(Jedis jedis, final String key, final Map<String, String> hash) {
        return jedis.hmset(key, hash);
    }

    private List<String> hvals(Jedis jedis, final String key) {
        return jedis.hvals(key);
    }

    private String hget(Jedis jedis, final String key, final String field) {
        return jedis.hget(key, field);
    }

    private ScanResult<Map.Entry<String, String>> hscan(Jedis jedis, final String key, final String cursor,
                                                        final ScanParams params) {
        return jedis.hscan(key, cursor, params);
    }

    private Long hincrBy(Jedis jedis, final String key, final String field, final long value) {
        return jedis.hincrBy(key, field, value);
    }

    private Boolean hexists(Jedis jedis, final String key, final String field) {
        return jedis.hexists(key, field);
    }

    private Long incr(Jedis jedis, final String key) {
        return jedis.incr(key);
    }

    private Long incrBy(Jedis jedis, final String key, final long increment) {
        return jedis.incrBy(key, increment);
    }

    private Long hdel(Jedis jedis, final String key, final String... fields) {
        return jedis.hdel(key, fields);
    }

    private Long sadd(Jedis jedis, final String key, final String... members) {
        return jedis.sadd(key, members);
    }

    private Boolean exists(Jedis jedis, final String key) {
        return jedis.exists(key);
    }

    private Long expire(Jedis jedis, final String key, final int seconds) {
        return jedis.expire(key, seconds);
    }
}

