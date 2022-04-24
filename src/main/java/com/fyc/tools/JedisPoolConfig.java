package com.fyc.tools;


import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JedisPoolConfig extends GenericObjectPoolConfig {
    public JedisPoolConfig() {
        this.setTestWhileIdle(true);
        this.setMinEvictableIdleTimeMillis(60000L);
        this.setTimeBetweenEvictionRunsMillis(30000L);
        this.setNumTestsPerEvictionRun(-1);
    }

    /**
     * 为什么要自己实现一个JedisPoolConfig呢，因为服务器上的
     * org.apache.common.pool2.BaseObject这个类，没有正确的toString方法。
     * @return 返回一个字符串。包含所有JedisPool的信息
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getClass().getSimpleName());
        builder.append(" [");
        this.toStringAppendFields(builder);
        builder.append("]");
        return builder.toString();
    }

    /**
     * 为什么要自己实现一个，见toString
     * @param builder
     */
    @Override
    protected void toStringAppendFields(StringBuilder builder) {
        builder.append("lifo=");
        builder.append(this.getLifo());
        builder.append(", fairness=");
        builder.append(this.getFairness());
        builder.append(", maxWaitMillis=");
        builder.append(this.getMaxWaitMillis());
        builder.append(", minEvictableIdleTimeMillis=");
        builder.append(this.getMinEvictableIdleTimeMillis());
        builder.append(", softMinEvictableIdleTimeMillis=");
        builder.append(this.getSoftMinEvictableIdleTimeMillis());
        builder.append(", numTestsPerEvictionRun=");
        builder.append(this.getNumTestsPerEvictionRun());
        builder.append(", evictionPolicyClassName=");
        builder.append(this.getEvictionPolicyClassName());
        builder.append(", testOnCreate=");
        builder.append(this.getTestOnCreate());
        builder.append(", testOnBorrow=");
        builder.append(this.getTestOnBorrow());
        builder.append(", testOnReturn=");
        builder.append(this.getTestOnReturn());
        builder.append(", testWhileIdle=");
        builder.append(this.getTestWhileIdle());
        builder.append(", timeBetweenEvictionRunsMillis=");
        builder.append(this.getTimeBetweenEvictionRunsMillis());
        builder.append(", blockWhenExhausted=");
        builder.append(this.getBlockWhenExhausted());
        builder.append(", jmxEnabled=");
        builder.append(this.getJmxEnabled());
        builder.append(", jmxNamePrefix=");
        builder.append(this.getJmxNamePrefix());
        builder.append(", jmxNameBase=");
        builder.append(this.getJmxNameBase());
        builder.append(", maxTotal=");
        builder.append(this.getMaxTotal());
        builder.append(", maxIdle=");
        builder.append(this.getMaxIdle());
        builder.append(", minIdle=");
        builder.append(this.getMinIdle());
    }
}