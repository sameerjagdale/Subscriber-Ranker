package phase2;
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
 * Class implements the phase 2 reducer for the program. 
 * Generates a count of the number of unique timestamps for a single day for a single 
 * key. 
 * @author Sameer Jagale
 */
public class Phase2Reducer extends
		Reducer<SubscriberInfo, IntWritable, SubscriberInfo, IntWritable> {
/** 
 * result. Holds the count of the number of unique time stamps for a day for every 
 * subscriber. 
 */
	private IntWritable result = new IntWritable();
/** 
 * The method is overriden from the Reducer class. Implements the reduce functionality of
 * phase 2.
 */
	@Overriden
	public void reduce(SubscriberInfo key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
