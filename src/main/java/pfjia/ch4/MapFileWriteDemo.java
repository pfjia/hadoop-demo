package pfjia.ch4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

/**
 * @author pfjia
 * @since 2018/4/4 9:17
 */
public class MapFileWriteDemo {
    public static void main(String[] args) throws IOException {
        String uri="hdfs://localhost/numbers.map";
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(URI.create(uri),conf);

        IntWritable key=new IntWritable();
        Text value=new Text();
        MapFile.Writer writer=null;
        try {
            writer=new MapFile.Writer(conf,fs,uri,key.getClass(),value.getClass());
            for (int i=0;i<1024;i++){
                key.set(i+1);;
                value.set(SequenceFileWriteDemo.DATA[i%SequenceFileWriteDemo.DATA.length]);
                writer.append(key,value);
            }
        }finally {
            IOUtils.closeStream(writer);
        }
    }
}
