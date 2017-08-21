package homework.hadoop;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class DescendingComparator extends WritableComparator {

    public DescendingComparator() {
        super(IntWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return delegate.compare(b2, s2, l2, b1, s1, l1);
    }

    IntWritable.Comparator delegate = new IntWritable.Comparator();
}
