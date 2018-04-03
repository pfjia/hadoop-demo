package pfjia.ch4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @author pfjia
 * @since 2018/4/3 10:28
 */
public class StreamCompressor {
    public static void main(String[] args) throws ClassNotFoundException, IOException {
        String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
        Class<?> codecClass = Class.forName(codecClassname);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());
        CompressionOutputStream out = codec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in,out,4096,false);
        out.finish();
    }
}
