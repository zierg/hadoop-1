package test.homework.hadoop;

import homework.hadoop.WordReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class WordReducerTest {

    @Test
    public void testLongestWord() throws IOException {
        List<NullWritable> nullWritable = Arrays.asList(NullWritable.get(), NullWritable.get());
        new ReduceDriver<Text, NullWritable, Text, NullWritable>()
                .withReducer(new WordReducer())
                .withInput(new Text("rabbit"), nullWritable)
                .withInput(new Text("parrot"), nullWritable)
                .withInput(new Text("doge"), nullWritable)
                .withInput(new Text("frog"), nullWritable)
                .withOutput(new Text("rabbit"), NullWritable.get())
                .withOutput(new Text("parrot"), NullWritable.get())
                .runTest(false);
    }
}
