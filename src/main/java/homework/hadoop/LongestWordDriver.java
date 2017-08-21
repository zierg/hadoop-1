package homework.hadoop;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class LongestWordDriver extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "The longest word");
        job.setJarByClass(LongestWordDriver.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setSortComparatorClass(DescendingComparator.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        Path outputDir = new Path(strings[1]);
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);
        LongestWordDriver driver = new LongestWordDriver();
        driver.setConf(conf);
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }
}
