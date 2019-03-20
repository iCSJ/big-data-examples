package com.andy.hbase.bloom;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.UUID;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-24
 **/
public class BloomFilterTest {

    private static final Logger logger = LoggerFactory.getLogger(BloomFilterTest.class);

    @Test
    public void bloomTest1() {
        // 一百万个数字
        int size = 1000000;

        BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), size);

        for (int i = 0; i < size; i++) {
            bloomFilter.put(UUID.randomUUID().toString());
        }

        boolean flag = false;

        long startTime = System.nanoTime();
        // 判断这一百万个字符串中是否包含 （hello） 这个字符串
        if (bloomFilter.mightContain("hello")) {
            flag = true;
        }

        long endTime = System.nanoTime();
        logger.info("result: {} time: {} 纳秒", flag, (endTime - startTime));
    }


    @Test
    public void bloomTest2() {
        int size = 1000000;

        BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), size, 0.01);

        for (int i = 0; i < size; i++) {
            bloomFilter.put(i);
        }

        int errorCount = 0;
        // 故意取100000个不在过滤器里的值，看看有多少个会被认为在过滤器里
        for (int i = size + 100000; i < size + 200000; i++) {
            if (bloomFilter.mightContain(i)) {
                errorCount++;
            }
        }

        double d = (double) errorCount / 10000;
        DecimalFormat df = new DecimalFormat("0.00000");
        df.format(d);

        logger.info("error count {} 错误率是 {}%", errorCount, df.format(d));
    }


}
