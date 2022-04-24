package com.fyc.tools;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
//import redis.clients.jedis.exceptions.JedisException;

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Created by yanxb on 2019/8/18.
 * 2020年6月12日
 * redis资源池设计思路
 * 相同的配置信息，保持一个池。
 * 静态变量保存池。
 */
public class JedisPoolUtil {
    private static final Map<Integer, JedisPool> poolSet = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(JedisPoolUtil.class);

    /**
     * 获取默认的资源池，所有连接信息来自配置文件
     *
     * @return 返回配置文件中设定的默认redis连接池
     */
    public static JedisPool GetPool() {
        return GetPool(host, port, timeout, password);
    }

    /**
     * 获取默认的资源池，所有连接信息来自配置文件，指定超时时间。
     *
     * @param timeout 指定超时时间。
     * @return 返回配置文件中设定的默认redis连接池
     */
    public static JedisPool GetPool(Integer timeout) {
        return GetPool(host, port, timeout, password);
    }

    /**
     * 生成连接池的字符串id
     *
     * @param jedisPoolConfig 连接池配置
     * @param hostName        hostName
     * @param hostPort        port
     * @param timeout         超时时间
     * @param password        密码
     * @return 返回参数的拼接字符串
     */
    private static String buildPoolId(JedisPoolConfig jedisPoolConfig, String hostName, Integer hostPort, Integer timeout, String password) {
        // todo:这里有个问题，jedisPoolConfig理应toString出来是一致的，但是服务器上不一致。使用了Object的ToString。
        String poolId = "pool:" + jedisPoolConfig.toString()
                + ",host:" + hostName + ",port:" + hostPort + ",timeout:" + timeout
                + ",password:" + password;
        return poolId;
    }

    /**
     * 根据输入的参数获取一个连接池，根据所有参数都判断如果已存在对应连接池则返回已有连接池，
     * 否则申请初始化连接池，如果连接池数量超过最大值，则会抛出异常。
     *
     * @param host     host
     * @param port     port
     * @param timeout  timeout
     * @param password password
     * @return 根据输入参数返回一个可用的连接池
     */
    public static JedisPool GetPool(String host, Integer port, Integer timeout, String password) {
        if (host == null) {
            host = JedisPoolUtil.host;
        }
        if (port == null) {
            port = JedisPoolUtil.port;
        }
        if (timeout == null) {
            timeout = JedisPoolUtil.timeout;
        }
        if (password == null) {
            password = JedisPoolUtil.password;
        }
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(maxTotal);
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMaxWaitMillis(maxWaitMillis);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        String poolId = buildPoolId(jedisPoolConfig
                , host, port, timeout, password);
        Integer intPoolID = poolId.hashCode();
        if (!poolSet.containsKey(intPoolID)) {
            allocPool(jedisPoolConfig, host, port, timeout, password);
        }
        return poolSet.get(intPoolID);
    }

    /**
     * 初始化一个资源池到资源池列表中，当列表中资源池数量已经达到 maxPoolSize 时，抛出资源池超限异常
     *
     * @param jedisPoolConfig 资源池配置
     * @param host            host
     * @param port            port
     * @param timeout         超时时间
     * @param password        密码
     */
    private static void allocPool(JedisPoolConfig jedisPoolConfig, String host, int port, int timeout, String password) {
        if (poolSet.size() == maxPoolSize) {
            throw new JedisException("资源池的数量达到最大值：maxPoolSize：" + maxPoolSize
                    + "。请检查是否发生了重复初始化资源池问题。" +
                    host + ":" + port + " timeout:" + timeout + "pass:" + password
            );
        }
        synchronized (poolSet) {
            String poolId = buildPoolId(jedisPoolConfig
                    , host, port, timeout, password);
            Integer intPoolID = poolId.hashCode();
            if (poolSet.containsKey(intPoolID)) {
                return;
            }
            logger.info("初始化新线程池：" + jedisPoolConfig.toString() + "host:" + host + ":" + port + " timeout:" + timeout + "pass:" + password);
            JedisPool pool = new JedisPool(jedisPoolConfig, host, port, timeout, password);
            poolSet.put(intPoolID, pool);
        }
    }

    // private static JedisPool jedisPool;
    private static final String host;
    private static final Integer port;
    private static final Integer maxTotal;
    private static final Integer maxIdle;
    private static final Integer maxWaitMillis;
    private static final String password;
    private static final Integer timeout;
    /**
     * 程序最大的redis资源池数量（注意，不是池里的资源，是池的数量）
     */
    private static final Integer maxPoolSize;

    static {
        ResourceBundle rb = ResourceBundle.getBundle("jedis");
        maxTotal = Integer.parseInt(rb.getString("maxtotal"));
        maxIdle = 100;
        maxWaitMillis = Integer.parseInt(rb.getString("maxwaitmillis"));
        host = rb.getString("host");
        port = Integer.parseInt(rb.getString("port"));
        password = rb.getString("password");
        timeout = Integer.parseInt(rb.getString("timeout"));
        maxPoolSize = 10;
    }
}
