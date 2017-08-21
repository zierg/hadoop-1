package homework.hadoop;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class WordReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);

        try {
            Integer largestKey = null;
            while (context.nextKey()) {
                IntWritable currentKey = context.getCurrentKey();
                if (largestKey != null && largestKey > currentKey.get()) {
                    return;
                }
                if (largestKey == null) {
                    largestKey = currentKey.get();
                }
                reduce(currentKey, context.getValues(), context);
            }
        } finally {
            cleanup(context);
        }
    }
}
