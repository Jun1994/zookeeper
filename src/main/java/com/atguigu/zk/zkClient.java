package com.atguigu.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName zkClient
 * @Author maxingjun@xci96716.com
 * @Since 2023/3/22 17:20
 * @Description
 * @Version 1.0
 */
public class zkClient {
    private static String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private static int sessionTimeout = 4000;
    private ZooKeeper zkClient = null;

    /**
     * 连接zookeeper
     *
     * @throws IOException
     */
    @Test
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    /**
     * 前置执行连接zookeeper
     *
     * @throws IOException
     */
    @Before
    public void initConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            public void process(WatchedEvent watchedEvent) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println("process注册");
                System.out.println(watchedEvent.getType() + "--"
                        + watchedEvent.getPath());
                // 再次启动监听
                try {
                    List<String> children = zkClient.getChildren("/",true);
                    for (String child : children) {
                        System.out.println("注册监听"+child);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 创建zookeeper节点
     */
    @Test
    public void create() throws InterruptedException, KeeperException {
//        String s = zkClient.create("/sanguo/shuguoer", "张三".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String s = zkClient.create("/sanguo/shuguowu", "赵云".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }


    /**
     * 监听”/“路径节点变化
     * @param path 监听的路径
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/", true);
        System.out.println("监听");
//        children.forEach((e) -> {
//            System.out.println("子节点监听"+e);
//        });

        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 判断节点是否存在
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void getExists() throws InterruptedException, KeeperException {
        Stat exists = zkClient.exists("/atguigu", false);
        System.out.println( exists==null ? "Exist":"Not Exist");
    }
}
