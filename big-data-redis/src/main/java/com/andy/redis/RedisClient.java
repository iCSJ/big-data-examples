package com.andy.redis;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-19
 **/
public class RedisClient {

    private static final String HOST = "127.0.0.1";

    private static final Integer PORT = 6379;

    private static Jedis jedis = new Jedis(HOST, PORT);


    public static void main(String[] args) {

    }

    @Before
    public void init() throws Exception {

    }


    @Test
    public void testSetString() {
        jedis.set("k1", "v1");
    }


}
