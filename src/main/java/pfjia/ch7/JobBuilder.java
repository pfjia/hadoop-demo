package pfjia.ch7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author pfjia
 * @since 2018/4/4 19:22
 */
public class JobBuilder {
    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public static void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
        ToolRunner.printGenericCommandUsage(System.err);
    }
}
