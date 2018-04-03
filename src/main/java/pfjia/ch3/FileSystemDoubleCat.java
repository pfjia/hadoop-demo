package pfjia.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * @author pfjia
 * @since 2018/4/3 9:42
 */
public class FileSystemDoubleCat {
    public static void main(String[] args) throws IOException {
        String url="hdfs://localhost:9000/user/pfjia/NOTICE.txt";
        FileSystem fs=FileSystem.get(URI.create(url),new Configuration());
        FSDataInputStream in=null;
        try {
            in=fs.open(new Path(url));
            IOUtils.copyBytes(in,System.out,4096,false);
            in.seek(0);
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
