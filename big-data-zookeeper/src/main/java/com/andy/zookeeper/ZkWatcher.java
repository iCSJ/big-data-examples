package com.andy.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-28
 **/
public class ZkWatcher implements Watcher {

    private final static Logger logger = LoggerFactory.getLogger(ZkWatcher.class);

    private static final String host = "xxx.xxx.xxx.xxx:2181";

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper;

    static {
        try {
            zooKeeper = new ZooKeeper(host, 5000, new ZkWatcher());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info("event {}", watchedEvent);
        // 连接状态
        Event.KeeperState keeperState = watchedEvent.getState();

        // 事件类型
        Event.EventType eventType = watchedEvent.getType();

        // 受影响的path
        String path = watchedEvent.getPath();

        if (Event.KeeperState.SyncConnected == keeperState) {
            logger.info("connection zookeeper success... keeperState: {} eventType: {} watchedEvent: {}", keeperState, eventType, path);
        }

        try {
            // 第二个参数的意思是使用默认的watch监听
            zooKeeper.exists("/aa", true);
            zooKeeper.create("/aa", "100".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            // 重新设置watch，zookeeper中watch被调用之后需要重新设置
            zooKeeper.exists("/aa", true);
            zooKeeper.delete("/aa", -1);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }


    }


    /**
     * @param path
     * @param watcher
     * @param stat
     * @return
     * @throws Exception
     */
    public byte[] getData(final String path, Watcher watcher, Stat stat) throws Exception {

        return null;
    }

}
