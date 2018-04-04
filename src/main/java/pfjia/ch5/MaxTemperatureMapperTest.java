package pfjia.ch5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.jupiter.api.Test;
import pfjia.ch2.MaxTemperatureMapper;

import java.io.IOException;

/**
 * @author pfjia
 * @since 2018/4/4 0:20
 */
public class MaxTemperatureMapperTest {
    @Test
    void processesValidRecord() throws IOException {
        Text value = new Text("0067011990999991950051507004+68750+023550FM-12+038299999V0203301N00671220001CN9999999N9+00001+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(1), value)
                .withOutput(new Text("1950"), new IntWritable(0))
                .runTest();
    }
}
