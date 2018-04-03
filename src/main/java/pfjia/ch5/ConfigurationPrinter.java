package pfjia.ch5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * @author pfjia
 * @since 2018/4/4 0:13
 */
public class ConfigurationPrinter extends Configured implements Tool {
    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            System.out.printf("%s=%s\n",entry.getKey(),entry.getValue());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode= ToolRunner.run(new ConfigurationPrinter(),args);
        System.exit(exitCode);
    }


}
