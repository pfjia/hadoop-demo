package pfjia.ch14;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;

/**
 * @author pfjia
 * @since 2018/4/9 12:39
 */
public class ActiveKeyValueStore extends ConnectionWatcher {
    private static final Charset CHARSET=Charset.forName("UTF-8");

    public void write(String path,String value) throws KeeperException, InterruptedException {
        Stat stat=zk.exists(path,false);
        if (stat==null){
            zk.create(path,value.getBytes(CHARSET),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }else {
            zk.setData(path,value.getBytes(CHARSET),-1);
        }
    }
}
