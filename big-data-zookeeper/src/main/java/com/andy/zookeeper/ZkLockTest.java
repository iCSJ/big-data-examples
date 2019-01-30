package com.andy.zookeeper;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-30
 **/
public class ZkLockTest {


    /**
     * 代表复杂逻辑执行了一段时间
     */
    private static void process() throws InterruptedException {
        System.out.println("----------业务开始----------");
        Thread.sleep((long) ((Math.random() * 2000)));
        System.out.println("----------业务结束----------");
    }


    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 100; i++) {
            ZkLock lock = new ZkLock();
            lock.acquireLock();
            process();
            lock.releaseLock();
        }
    }


}
