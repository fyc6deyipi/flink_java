package com.fyc.tools;

import java.util.ResourceBundle;

public class REDIS_KEYS {
    public static final ResourceBundle rb = ResourceBundle.getBundle("jedis_key");

    public static final String REDIS_GTW_COUNT=rb.getString("REDIS_GTW_COUNT");
    public static final String REDIS_GTW_SOURCE_COUNT=rb.getString("REDIS_GTW_SOURCE_COUNT");
    public static final String REDIS_ALIYUN_SOURCES_COUNT=rb.getString("REDIS_ALIYUN_SOURCES_COUNT");
    public static final String REDIS_ALIYUN_SOURCES_USER_COUNT=rb.getString("REDIS_ALIYUN_SOURCES_USER_COUNT");
    public static final String REDIS_ALIYUN_SOURCES_BEHAVIOR=rb.getString("REDIS_ALIYUN_SOURCES_BEHAVIOR");


}
