package com.atguigu.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName DistributeClient
 * @Author maxingjun@xci96716.com
 * @Since 2023/3/23 14:53
 * @Description
 * @Version 1.0
 */
public class DistributeClient {
    public static String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    public static Integer sessionTimeout = 2000;
    public ZooKeeper zkClient = null;
    private String parentNode = "/servers";

    public static void main(String[] args) throws Exception {
        // 1 获取 zk 连接
        DistributeClient client = new DistributeClient();
        client.getConnect();
        // 2 获取 servers 的子节点信息，从中获取服务器信息列表
        client.getServerList();
        // 3 业务进程启动
        client.business();
    }

    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getServerList();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * 获取服务节点信息
     *
     * @throws Exception
     */
    public void getServerList() throws Exception {
        // 1 获取服务器子节点信息，并且对父节点进行监听
        List<String> zkClientChildren = zkClient.getChildren(parentNode, true);
        // 2 存储服务器信息列表
        ArrayList<String> servers = new ArrayList<>();
        // 3 遍历所有节点，获取节点中的主机名称信息
        for (String child : zkClientChildren) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        // 4 打印服务器列表信息
        System.out.println(servers);
    }

    /**
     * 客户端启动控制台输出
     *
     * @throws InterruptedException
     */
    public void business() throws InterruptedException {
        System.out.println("Client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }
}
