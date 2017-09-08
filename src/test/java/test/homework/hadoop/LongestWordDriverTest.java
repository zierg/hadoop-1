package test.homework.hadoop;

import homework.hadoop.WordLengthDescendingComparator;
import homework.hadoop.WordMapper;
import homework.hadoop.WordReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

import java.io.IOException;

public class LongestWordDriverTest {

    @Test
    public void test() throws IOException {
        Text value = new Text("one two three four five six seven eight nine ten");
        //noinspection unchecked
        RawComparator<Text> comparator = new WordLengthDescendingComparator();
        MapReduceDriver.<Object, Text, Text, NullWritable, Text, NullWritable>newMapReduceDriver()
                .withMapper(new WordMapper())
                .withKeyOrderComparator(comparator)
                .withCombiner(new WordReducer())
                .withReducer(new WordReducer())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("three"), NullWritable.get())
                .withOutput(new Text("seven"), NullWritable.get())
                .withOutput(new Text("eight"), NullWritable.get())
                .runTest(false);
    }
}
