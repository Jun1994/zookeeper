package com.atguigu.lock1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @ClassName DistributedLock
 * @Author maxingjun@xci96716.com
 * @Since 2023/3/23 16:29
 * @Description
 * @Version 1.0
 */
public class DistributedLock {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    private String rootNode = "locks";
    private String subNode = "seq-";
    private String waitPath;

    private CountDownLatch connectLatch = new CountDownLatch(1);

    //zookeeper节点等待
    private CountDownLatch waitLatch = new CountDownLatch(1);
    //当前client创建的子节点
    private String currentNode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 连接建立时, 打开 latch, 唤醒 wait 在该 latch 上的线程
                if(event.getState()==Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                }
                // 发生了 waitPath 的删除事件
                if(event.getType()==Event.EventType.NodeDeleted && event.getPath().equals(waitPath)){
                    waitLatch.countDown();
                }
            }
        });
        // 等待连接建立
        connectLatch.await();
        //获取根节点状态
        Stat stat = zk.exists("/" + rootNode, false);
        //如果根节点不存在，则创建根节点，根节点类型为永久节点
        if (stat == null) {
            System.out.println("根节点不存在");
            zk.create("/" + rootNode, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // 加锁方法
    public void zkLock(){
        try {
            currentNode = zk.create("/"+rootNode+"/"+subNode,null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            //wait一小会，等待创建完毕
            Thread.sleep(10);
            //注意，没有必要监听"/locks"的子节点的变化情况
            List<String> childrenNodes = zk.getChildren("/" + rootNode, false);
            //列表中只有一个子节点，那肯定就是currentNode，说明client获得锁
            if (childrenNodes.size()==1){
                return;
            }else {
                //对根节点下的所有临时顺序节点进行从小到大排序
                Collections.sort(childrenNodes);
                //当前节点名称
                String thisNode = currentNode.substring(("/" + rootNode + "/").length());
                int index = childrenNodes.indexOf(thisNode);
                if(index == -1){
                    System.out.println("数据异常");
                } else if (index == 0) {
                    //index = 0 ,说明thisNode在列表中最小，当前client获得锁
                    return;
                }else {
                    //获得排名比currentNode前1位的节点
                    this.waitPath = "/"+rootNode+"/"+childrenNodes.get(index-1);
                    //在waitPath上注册监听器，当waitPath被删除时，zookeeper会回调监听器的process方法
                    zk.getData(waitPath,true,new Stat());
                    //进入等待锁状态
                    waitLatch.await();
                    return;
                }
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // 解锁方法
    public void zkUnlock() {
        try {
            zk.delete(this.currentNode, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
