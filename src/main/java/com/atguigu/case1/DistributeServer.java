package com.atguigu.case1;

import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * @ClassName DistributeServer
 * @Author maxingjun@xci96716.com
 * @Since 2023/3/23 11:12
 * @Description
 * @Version 1.0
 */
public class DistributeServer {
    public static String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    public static Integer sessionTimeout = 2000;
    public ZooKeeper zkServer = null;
    private String parentNode = "/servers/";

    public static void main(String[] args) throws Exception {
        // 1 获取 zk 连接
        DistributeServer server = new DistributeServer();
        server.getConnect();
        // 2 利用 zk 连接注册服务器信息
        server.registServer(args[0]);
        // 3 启动业务功能
        server.business(args[0]);
    }

    public void getConnect() throws IOException {
        zkServer = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    public void registServer(String hostname) throws InterruptedException, KeeperException {
        String create = zkServer.create(parentNode+hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname +" is online "+ create);
    }

    public void business(String hostname) throws InterruptedException {
        System.out.println(hostname + " is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }
}
