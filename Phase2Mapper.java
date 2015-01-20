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

public class Phase2Mapper extends Mapper<Object, Text, SubscriberInfo, IntWritable> {
		private static IntWritable one =  new IntWritable(1);
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
					context.write(SubscriberInfo.genSubscriberInfo(value.toString()),one);
		}
}
