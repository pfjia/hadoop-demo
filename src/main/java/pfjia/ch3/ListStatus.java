package pfjia.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @author pfjia
 * @since 2018/4/3 9:49
 */
public class ListStatus {
    public static void main(String[] args) throws IOException {
        String uri="hdfs://localhost:9000/";
        FileSystem fs=FileSystem.get(URI.create(uri),new Configuration());
        Path[] paths=new Path[2];
        paths[0]=new Path("hdfs://localhost:9000/");
        paths[1]=new Path("hdfs://localhost:9000/user/pfjia");
        FileStatus[]statuses=fs.listStatus(paths);
        Path[] listedPaths=FileUtil.stat2Paths(statuses);
        for (Path listedPath : listedPaths) {
            System.out.println(listedPath);
        }
    }
}
