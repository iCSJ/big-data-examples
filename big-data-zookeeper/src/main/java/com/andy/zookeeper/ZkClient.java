package com.andy.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author leone
 * @since 2018-06-16
 **/
public class ZkClient {

    private static CountDownLatch countDownLatch=new CountDownLatch(1);

    private final static String zk_url="39.108.125.41:2183";

    private final static int time_out=5000;

    public static void main(String[] args) throws IOException, InterruptedException {
        //初始化zk
        ZooKeeper zooKeeper=new ZooKeeper(zk_url, time_out, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                Event.KeeperState state = watchedEvent.getState();
                Event.EventType type = watchedEvent.getType();
                if(Event.KeeperState.SyncConnected==state){
                    if(Event.EventType.None==type){
                        //调用此方法测计数减一
                        countDownLatch.countDown();
                    }
                }
            }
        });
        //阻碍当前线程进行,除非计数归零
        countDownLatch.await();
        try {
            //创建持久化节点
            zooKeeper.create("/com.andy","你好".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //获取节点数据
            byte[] data = zooKeeper.getData("/com.andy", false, null);
            System.out.println(new String(data));
            //修改节点数据
            zooKeeper.setData("/com.andy","james".getBytes(),0);
            //删除节点数据
            zooKeeper.delete("/com.andy",-1);
            //创建临时节点 异步创建
            zooKeeper.create("/com.andy", "tmp".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {
                public void processResult(int i, String s, Object o, String s1) {
                    System.out.println(o);
                    System.out.println(i);
                    System.out.println(s1);
                    System.out.println(s);
                }
            },"a");
            //获取临时节点数据
            byte[] jingangs = zooKeeper.getData("/com.andy", false, null);
            System.out.println(new String(jingangs));
            //验证节点是否存在
            Stat exists = zooKeeper.exists("/com.andy", false);
            System.out.println(exists);
        } catch (Exception e) {
            e.printStackTrace();
        }
        zooKeeper.close();
    }


}