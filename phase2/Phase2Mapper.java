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
 * Class implements the map operation for the second phase of the map
 * operation. It reads entire lines from the file and outputs the SubscriberInfo object 
 * as key and and an IntWritable as value.
 * @author Sameer Jagdale.
 */
public class Phase2Mapper extends
		Mapper<Object, Text, SubscriberInfo, IntWritable> {
/** 
 * one. static variable initialised to one which is used as the output value of the map
 * operation. 
 */
	private static IntWritable one = new IntWritable(1);

/** 
 * map. The method is overriden from the Mapper class. Implements the map functionality 
 * of phase 2. 
 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(SubscriberInfo.genSubscriberInfo(value.toString()), one);
	}
}
