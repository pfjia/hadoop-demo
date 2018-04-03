package pfjia.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * @author pfjia
 * @since 2018/4/3 9:45
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws IOException {
        String localSrc="D:/hadoop-3.0.1/NOTICE.txt";
        String dst="hdfs://localhost:9000/NOTICE.txt";
        InputStream in=new BufferedInputStream(new FileInputStream(localSrc));
        FileSystem fs=FileSystem.get(URI.create(dst),new Configuration());
        OutputStream out=fs.create(new Path(dst), () -> System.out.print("."));
        IOUtils.copyBytes(in,out,4096,true);
    }
}
