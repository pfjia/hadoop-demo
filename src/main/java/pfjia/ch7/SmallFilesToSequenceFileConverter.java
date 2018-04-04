package pfjia.ch7;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @author pfjia
 * @since 2018/4/4 20:08
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    public static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
        private Text filenameKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split=context.getInputSplit();
            Path path =((FileSplit)split).getPath();
            filenameKey=new Text(path.toString());
        }
    }

    public static void main(String[] args) {

    }
}
