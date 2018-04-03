package pfjia.ch4;

import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.File;
import java.io.IOException;

/**
 * @author pfjia
 * @since 2018/4/3 20:24
 */
public class AvroSort extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        String input = args[0];
        String output = args[1];
        String schemaFile = args[2];

        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("Avro sort");

        FileInputFormat.addInputPath(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        Schema schema = new Schema.Parser().parse(new File(schemaFile));
        AvroJob.setInputSchema(conf, schema);
        Schema intermediateSchema = Pair.getPairSchema(schema, schema);
        AvroJob.setMapOutputSchema(conf, intermediateSchema);
        AvroJob.setOutputSchema(conf, schema);

        AvroJob.setMapperClass(conf, SortMapper.class);
        AvroJob.setReducerClass(conf, SortReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroSort(), args);
        System.exit(exitCode);
    }

    public static class SortMapper<K> extends AvroMapper<K, Pair<K, K>> {
        @Override
        public void map(K datum, AvroCollector<Pair<K, K>> collector, Reporter reporter) throws IOException {
            collector.collect(new Pair<>(datum, null, datum, null));
        }
    }

    public static class SortReducer<K> extends AvroReducer<K, K, K> {
        @Override
        public void reduce(K key, Iterable<K> values, AvroCollector<K> collector, Reporter reporter) throws IOException {
            for (K value : values) {
                collector.collect(value);
            }
        }
    }
}
