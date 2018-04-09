package pfjia.ch14;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author pfjia
 * @since 2018/4/9 12:41
 */
public class ConfigUpdater {
    public static final String PATH="/config";

    private ActiveKeyValueStore store;
    private Random random=new Random();

    public ConfigUpdater(String hosts) throws IOException {
        store=new ActiveKeyValueStore();
        store.connect(hosts);
    }

    public void run() throws KeeperException, InterruptedException {
        while (true){
            String value=random.nextInt(100)+"";
            store.write(PATH,value);
            System.out.printf("Set %s to %s\n",PATH,value);
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ConfigUpdater updater=new ConfigUpdater(args[0]);
        updater.run();
    }
}
