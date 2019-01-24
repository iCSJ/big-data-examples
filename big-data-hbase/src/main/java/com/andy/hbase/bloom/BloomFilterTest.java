package com.andy.hbase.bloom;

import com.andy.hbase.HBaseCrudTest;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-24
 **/
public class BloomFilterTest {


    private static final Logger logger = LoggerFactory.getLogger(HBaseCrudTest.class);


    @Test
    public void bloomTest1() {
        int size = 1000000;

        BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), size);

        for (int i = 0; i < size; i++) {
            bloomFilter.put(i);
        }
        boolean flag = false;
        long startTime = System.nanoTime();
        // 判断这一百万个数中是否包含29999这个数
        if (bloomFilter.mightContain(29999)) {
            flag = true;
        }
        long endTime = System.nanoTime();
        logger.info("result {} time {} 纳秒", flag, (endTime - startTime));
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
        logger.info("error count {} 错误率是 {}%", errorCount, ((double) errorCount / 10000) * 100);
    }


}
