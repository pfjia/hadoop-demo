package pfjia.ch14;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

/**
 * @author pfjia
 * @since 2018/4/9 9:43
 */
public class DeleteGroup extends ConnectionWatcher {

    public void delete(String groupName) throws KeeperException, InterruptedException {
        String path="/"+groupName;
        try {
            List<String > children=zk.getChildren(path,false);
            for (String child : children) {
                zk.delete(path+"/"+child,-1);
            }
        }catch (KeeperException.NoNodeException e){
            System.out.printf("Group %sd does not exist\n",groupName);
            System.exit(-1);
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DeleteGroup deleteGroup=new DeleteGroup();
        deleteGroup.connect(args[0]);
        deleteGroup.delete(args[1]);
        deleteGroup.close();
    }
}
