package homework.hadoop;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Compares words by their lengths and then, if the lengths are equal, compares the words by alphabetical order
 */
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class WordLengthDescendingComparator extends WritableComparator {

    protected WordLengthDescendingComparator() {
        super(Text.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int n1 = WritableUtils.decodeVIntSize(b1[s1]);
        int n2 = WritableUtils.decodeVIntSize(b2[s2]);
        int length1 = l1 - n1;
        int length2 = l2 - n2;
        int difference = length2 - length1;
        return difference != 0 ? difference : delegate.compare(b1, s1, l1, b2, s2, l2);
    }

    Text.Comparator delegate = new Text.Comparator();
}
