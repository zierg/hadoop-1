package test.homework.hadoop;

import homework.hadoop.WordMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class WordMapperTest {

    @Test
    public void test() throws IOException {
        Text value = new Text("one two three four five six seven eight nine ten");
        new MapDriver<Object, Text, IntWritable, Text>()
                .withMapper(new WordMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new IntWritable(5), new Text("three"))
                .withOutput(new IntWritable(5), new Text("seven"))
                .withOutput(new IntWritable(5), new Text("eight"))
                .runTest(false);
    }
}