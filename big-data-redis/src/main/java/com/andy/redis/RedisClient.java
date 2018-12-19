package com.andy.redis;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-19
 **/
public class RedisClient {

    private static final String HOST = "127.0.0.1";

    private static final Integer PORT = 6379;

    private static final Integer TIME_OUT = 15000;

    private static Jedis jedis = null;


    private static final JedisPool jedisPool;

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        //最大连接数
        config.setMaxTotal(32);
        //闲置最大连接数
        config.setMaxIdle(6);
        //闲置最小连接数
        config.setMinIdle(0);
        //到达最大连接数后，调用者阻塞时间
        config.setMaxWaitMillis(15000);
        //连接空闲的最小时间，可能被移除
        config.setMinEvictableIdleTimeMillis(300000);
        //连接空闲的最小时间，多余最小闲置连接的将被移除
        config.setSoftMinEvictableIdleTimeMillis(-1);
        //设置每次检查闲置的个数
        config.setNumTestsPerEvictionRun(3);
        //申请连接时，是否检查连接有效
        config.setTestOnBorrow(false);
        //返回连接时，是否检查连接有效
        config.setTestOnReturn(false);
        //空闲超时,是否执行检查有效
        config.setTestWhileIdle(false);
        //空闲检查时间
        config.setTimeBetweenEvictionRunsMillis(60000);
        //当连接数耗尽，是否阻塞
        config.setBlockWhenExhausted(true);
        //连接池配置对象 (host + port + timeout + password + db)
        jedisPool = new JedisPool(config, HOST, PORT, TIME_OUT, null, 1);
        jedis = jedisPool.getResource();
    }


    /**
     * set and get string
     */
    @Test
    public void testString() {
        System.out.println("----------------redis-String-----------------");
        //set:返回操作结果
        System.out.println("name=>wsy:" + jedis.set("name", "wsy"));

        //get:value
        System.out.println("name:" + jedis.get("name"));

        //append:字符串长度
        System.out.println("append:" + jedis.append("name", "_ss"));

        //strlen:字符串长度
        System.out.println("strlen:" + jedis.strlen("name"));

        //getrange:返回不包括起始坐标的值
        System.out.println("getrange:" + jedis.getrange("name", 10, 13));

        //setrange:从起始坐标考试替换，未替换的保持
        System.out.println("setrange:" + jedis.setrange("name", 10, "#"));

        //mset:批量设置，返回批量设置结果
        System.out.println("mset:" + jedis.mset("name", "wsy", "age", "29"));

        //mget:返回数组
        System.out.println("mget:" + jedis.mget("name", "age"));

        //incr:value自增1后，返回value
        System.out.println("incr:" + jedis.incr("age"));

        //incr:value自增传参值后，返回value
        System.out.println("incrBy:" + jedis.incrBy("age", 3));

        //decr:value自减1，返回value
        System.out.println("decr:" + jedis.decr("age"));

        //decrBy:value自减入参值，返回value
        System.out.println("decrBy:" + jedis.decrBy("age", 3));

        //setex:设置key值+有效时间，如果key存在则覆盖value
        System.out.println("setex:" + jedis.setex("phone", 10, "13600000001"));

        //setnx:当key不存在时，设置才成功
        System.out.println("setnx:" + jedis.setnx("address", "china"));

        //del:删除对应key
        System.out.println("del:" + jedis.del("address1"));

        System.out.println("----------------redis-String-----------------\n");

    }


    /**
     * redis中hash类型常用操作
     */
    @Test
    public void testSetMap() {
        System.out.println("----------------redis-HashMap-----------------");
        //hset:返回值为key为新返回1，为旧覆盖旧值返回0
        System.out.println("hset:" + jedis.hset("user", "name", "wangshaoyi"));

        Map<String, String> map = new HashMap<>();

        map.put("name", "wsy");
        map.put("age", "29");

        //hmset:map对象
        System.out.println("hmset:" + jedis.hmset("user", map));

        //hexists:判断hashmap中key是否存在
        System.out.println("hexists:" + jedis.hexists("user", "age"));

        //hget:获取map中key对应的value
        System.out.println("hget:" + jedis.hget("user", "name"));

        //hgetAll:获取map中所有对象
        System.out.println("hgetAll:" + jedis.hgetAll("user"));

        //hkeys:获取map中所有key
        System.out.println("hkeys:" + jedis.hkeys("user"));

        //hvals:获取map中所有value
        System.out.println("hvals:" + jedis.hvals("user"));


        //hmget:批量获取keys的对象，返回List
        System.out.println("hmget:" + jedis.hmget("user", "age", "name"));

        //hlen:map的大小
        System.out.println("hlen:" + jedis.hlen("user"));

        //hdel:删除map中对应key,正确删除返回1
        System.out.println("hdel:" + jedis.hdel("user", "age0"));

        System.out.println("----------------redis-HashMap-----------------\n");

    }


}
