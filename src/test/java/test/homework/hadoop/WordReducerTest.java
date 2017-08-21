package test.homework.hadoop;

import homework.hadoop.WordReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import javax.util.streamex.StreamEx;
import java.io.IOException;
import java.util.List;

public class WordReducerTest {

    @Test
    public void testLongestWord() throws IOException {
        new ReduceDriver<IntWritable, Text, IntWritable, Text>()
                .withReducer(new WordReducer())
                .withInput(toInt(6), text("rabbit", "parrot"))
                .withInput(toInt(4), text("doge", "frog"))
                .withOutput(toInt(6), new Text("rabbit"))
                .withOutput(toInt(6), new Text("parrot"))
                .runTest(false);
    }

    private IntWritable toInt(int value) {
        return new IntWritable(value);
    }

    private List<Text> text(String... values) {
        return StreamEx.of(values)
                .map(Text::new)
                .toList();
    }
}