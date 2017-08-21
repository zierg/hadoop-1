package test.homework.hadoop;

import homework.hadoop.WordMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class WordMapperTest {

    @Test
    public void test() throws IOException {
        Text value = new Text("one two three four five six seven eight nine ten");
        new MapDriver<Object, Text, Text, NullWritable>()
                .withMapper(new WordMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("three"), NullWritable.get())
                .withOutput(new Text("seven"), NullWritable.get())
                .withOutput(new Text("eight"), NullWritable.get())
                .runTest(false);
    }
}
