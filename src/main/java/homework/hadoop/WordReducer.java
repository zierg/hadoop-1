package homework.hadoop;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class WordReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);

        try {
            Integer largestKeyLength = null;
            Text latestWord = null;
            while (context.nextKey()) {
                Text currentKey = context.getCurrentKey();
                if (largestKeyLength != null && largestKeyLength > currentKey.getLength()) {
                    return;
                }
                if (largestKeyLength == null) {
                    largestKeyLength = currentKey.getLength();
                }
                if (latestWord == null || !latestWord.equals(currentKey)) {
                    latestWord = new Text(currentKey);
                    reduce(currentKey, context.getValues(), context);
                }
            }
        } finally {
            cleanup(context);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
