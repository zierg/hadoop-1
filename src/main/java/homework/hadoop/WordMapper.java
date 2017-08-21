package homework.hadoop;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class WordMapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        int maxLength = 0;
        Set<String> words = new HashSet<>();
        while (itr.hasMoreTokens()) {
            String tokenWord = itr.nextToken();
            int length = tokenWord.length();
            maxLengthWritable.set(length);
            if (length >= maxLength) {
                if (length > maxLength) {
                    words.clear();
                }
                maxLength = length;
                words.add(tokenWord);
            }
        }
        writeWords(context, words);
    }

    private void writeWords(Context context, Set<String> words) {
        words.forEach(tokenWord -> writeWord(context, tokenWord));
    }

    private void writeWord(Context context, String tokenWord) {
        word.set(tokenWord);
        try {
            context.write(word, NullWritable.get());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    Text word = new Text();

    IntWritable maxLengthWritable = new IntWritable(1);
}
