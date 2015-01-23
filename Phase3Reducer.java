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

public class Phase3Reducer extends Reducer<Text, Text, Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
			String str = "";
			Set<ValueHolder> inputSet = new TreeSet<ValueHolder>();	
			for( Text txt :  values) {
				inputSet.add(ValueHolder.getValueHolderFromString(txt.toString()));	
			}			
			int numRecords = inputSet.size();
			for( ValueHolder value : inputSet) {
				long rank = (numRecords - value.getCount()) + 1;
				System.out.println(value.toString());
		 		context.write(key, new Text(value.toString()+ 
					"\t" + rank+ "/" + numRecords));	
			}
		}	
		
}

class ValueHolder implements Comparable<ValueHolder>{
		String serviceName;
		long subscriberId;
		String serviceType;
		long count;		
		ValueHolder(String serviceName, long subscriberId,String serviceType,
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
					if( obj == null) return false;
					if(!(obj instanceof ValueHolder)) return false;
					ValueHolder other = (ValueHolder) obj;
					return serviceName.equals(other.getServiceName()) &&
							serviceType.equals(other.getServiceType()) &&
							subscriberId == other.getSubscriberId() &&
							count == other.getCount();
			}

			public String toString() {
				return serviceName + "\t" + subscriberId + "\t" 
					+ serviceType + "\t" + count;	
			}

			@Override		
			public int hashCode() {
				return ((new Long(subscriberId)).hashCode()) +
					serviceName.hashCode() + serviceType.hashCode() +
					(new Long(count)).hashCode();
			}

			public int compareTo( ValueHolder other) {
				if((int)(other.getCount() - count) == 0) {
					return (int)(other.subscriberId - subscriberId);
				}
				return (int)(other.getCount() - count);
			}

			public static ValueHolder getValueHolderFromString(String str) {
				String [] fields = (str.toString().split("[\t ]+"));
				return new ValueHolder(fields[0], Long.parseLong(fields[1]), 
					fields[2], Long.parseLong(fields[3]));
			}
		}
