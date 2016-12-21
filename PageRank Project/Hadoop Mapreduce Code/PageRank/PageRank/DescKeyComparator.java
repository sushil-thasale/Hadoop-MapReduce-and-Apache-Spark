package PageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// used for sorting DoubleWritable page rank value
// in descending order
public class DescKeyComparator extends WritableComparator {

	protected DescKeyComparator() {
		super(DoubleWritable.class, true);
	}

	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		DoubleWritable num1 = (DoubleWritable) key1;
		DoubleWritable num2 = (DoubleWritable) key2;

		// Implement sorting in descending order
		int result=0;
		if(num1.get() == num2.get())
			result = 0;
		else if (num1.get() < num2.get())
			result = -1;
		else
			result = 1;

		return (-1*result);
	}
}
