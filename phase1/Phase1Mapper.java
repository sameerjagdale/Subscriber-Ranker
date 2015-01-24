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
 * Class implements the mapper for the first phase of the program. 
 * The class extends the Mapper class as required by hadoop. The class 
 * also overrides the map method. The current map operation accepts the 
 * an Object object has key and a Text object as value and outputs 
 * objects of class SubscriberAccessInfo as key and IntWritable as values. 
 * This map operation will group all the input records with the same timestamps
 * and the reduce operation will generate a single output for them. 
 * @author Sameer Jagdale
 */
public class Phase1Mapper extends
		Mapper<Object, Text, SubscriberAccessInfo, IntWritable> {
/** 
 * A static variable which is initialised to one. Passed as the output value for
 * every map call. 
 */
	private static IntWritable one = new IntWritable(1);

/** Method overriden from the Mapper class. Map functionality is implemented in this 
 * class. 
 * @param key Input key type of the method.
 * @param value Input value of the method. Represents a single line of a file. 
 * @param context Used to write to the output stream. 
 */
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(
				SubscriberAccessInfo.genSubscriberAccessInfo(value.toString()),
				one);
	}
}
