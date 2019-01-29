package com.andy.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Set;

/**
 * <p> redis 哨兵集群
 *
 * @author leone
 * @since 2019-01-29
 **/
public class RedisSentinelTest {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {

        Set<String> sentinels = new HashSet<>();

        sentinels.add("192.168.79.15:26379");
        sentinels.add("192.168.79.16:26379");
        sentinels.add("192.168.79.17:26379");

        String clusterName = "mymaster";
        String password = "123456";

        JedisSentinelPool redisSentinelPool = new JedisSentinelPool(clusterName, sentinels, password);

        Jedis jedis = null;
        try {
            jedis = redisSentinelPool.getResource();
            jedis.set("k10", "v10");
            System.out.println(jedis.get("k10"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            redisSentinelPool.returnBrokenResource(jedis);
        }
        redisSentinelPool.close();
    }


}