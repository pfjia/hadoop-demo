package pfjia.ch14;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author pfjia
 * @since 2018/4/9 9:06
 */
public class JoinGroup extends ConnectionWatcher {
    public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
        String path = "/" + groupName + "/" + memberName;
        String createdPath = zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Createdd " + createdPath);
    }

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect(args[0]);
        joinGroup.join(args[1], args[2]);

        // stay alive until process is killed or thrad is interrupted
        TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
    }
}
