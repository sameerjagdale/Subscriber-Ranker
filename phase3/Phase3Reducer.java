package phase3;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import subscriber.*;

/** 
 * The class implements the reducer for the last phase of the program. 
 * @author Sameer Jagdale
 */
public class Phase3Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String str = "";
		Set<ValueHolder> inputSet = new TreeSet<ValueHolder>();
		// Read values into a Tree Set. This ensures values are sorted. 
		// This provides ease while ranking the output. 
		for (Text txt : values) {
			inputSet.add(ValueHolder.getValueHolderFromString(txt.toString()));
		}
		// Defines the number of records per day. 
		int numRecords = inputSet.size();
		long diff = Integer.MIN_VALUE;
		long rank = 0;
		// Count will hold the number of records having the same count. 
		// This is required because the records having the same count 
		// should have the same rank. However the rank of subsequent 
		// records will have to incremented by the number of subscribers
		// with the count value. 
		long count = 1;
		for (ValueHolder value : inputSet) {
			if (diff != (numRecords - value.getCount()) + 1) {
				diff = numRecords - value.getCount() + 1;
				rank += count;
				count = 1;
			} else {
				count++;
			}
			context.write(key, new Text(value.toString() + "\t" + rank + "/"
					+ numRecords));
		}
	}

}

/** 
 * The class acts a placeholder for the values. Contains all the input fields
 * such as serviceName, subscriberId, serviceType and count. The compareTo method which
 * is overriden orders the objects first on count, followed by subscriberId, followed by 
 * serviceName and finally serviceType. 
 * @author Sameer Jagdale
 */
class ValueHolder implements Comparable<ValueHolder> {
	String serviceName;
	long subscriberId;
	String serviceType;
	long count;

	ValueHolder(String serviceName, long subscriberId, String serviceType,
			long count) {
		this.serviceName = serviceName;
		this.serviceType = serviceType;
		this.subscriberId = subscriberId;
		this.count = count;
	}

	public long getCount() {
		return count;
	}

	public String getServiceName() {
		return serviceName;
	}

	public String getServiceType() {
		return serviceType;
	}

	public long getSubscriberId() {
		return subscriberId;
	}

	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (!(obj instanceof ValueHolder))
			return false;
		ValueHolder other = (ValueHolder) obj;
		return serviceName.equals(other.getServiceName())
				&& serviceType.equals(other.getServiceType())
				&& subscriberId == other.getSubscriberId()
				&& count == other.getCount();
	}

	public String toString() {
		return serviceName + "\t" + subscriberId + "\t" + serviceType + "\t"
				+ count;
	}

	@Override
	public int hashCode() {
		return ((new Long(subscriberId)).hashCode()) + serviceName.hashCode()
				+ serviceType.hashCode() + (new Long(count)).hashCode();
	}

	public int compareTo(ValueHolder other) {
		if ((int) (other.getCount() - count) == 0) {
			if ((int) (subscriberId - other.subscriberId) == 0) {
				if (serviceName.compareTo(other.getServiceName()) == 0) {
					return serviceType.compareTo(other.getServiceType());
				} else {
					return serviceName.compareTo(other.getServiceName());
				}
			} else {
				return (int) (subscriberId - other.subscriberId);
			}
		} else {
			return (int) (other.getCount() - count);
		}
	}

	public static ValueHolder getValueHolderFromString(String str) {
		String[] fields = (str.toString().split("[\t ]+"));
		return new ValueHolder(fields[0], Long.parseLong(fields[1]), fields[2],
				Long.parseLong(fields[3]));
	}
}
