package phase1;
import java.io.IOException;
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
 * Reducer class for the first phase of the program. It reads objects of type
 * SubscriberAccessInfo from the map function and returns a count of the number of
 * non-unique timestamps for the current key. 
 * @author Sameer Jagdale
 */
public class Phase1Reducer
		extends
		Reducer<SubscriberAccessInfo, IntWritable, SubscriberAccessInfo, IntWritable> {
/**
 * result. Static variable which holds the count of the number of the non-unique 
 * timestamps for each key. 
 */
	private IntWritable result = new IntWritable();

/** 
 * The method is overriden from the Reducer class. Implements the functionality for the 
 * reduce operation. 
 */
 	@Overriden
	public void reduce(SubscriberAccessInfo key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
