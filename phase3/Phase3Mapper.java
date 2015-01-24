package phase3;
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
 * Class implemnts the mapper for the third and final phase of program. 
 * @author Sameer Jagdale
 */
public class Phase3Mapper extends Mapper<Object, Text, Text,Text> {
		private static IntWritable one =  new IntWritable(1);
		private Text word =  new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
					String fields[] = value.toString().split("[\t ]+");
					context.write(new Text(fields[3]),new Text(fields[0] + "\t" + Long.parseLong(fields[1])
					+ "\t" + fields[2] + "\t" + Integer.parseInt(fields[4])));
		}
}
