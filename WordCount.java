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

public class WordCount {
	final static  String intermediatePath = "intermediate";	
	 public static void main(String[] args) throws Exception {
		execPhase1(args);
		execPhase2(args);	
  	}

	public static void execPhase1(String args[]) throws Exception{
		Job job = setupClass(new Configuration(), "Phase 1", WordCount.class, TokenizerMapper.class,
			IntReducer.class, IntReducer.class, SubscriberAccessInfo.class, IntWritable.class, 
			args[0], intermediatePath);
		job.waitForCompletion(true);
	}

	public static void execPhase2(String args[]) throws Exception{
		Job job = setupClass(new Configuration(), "Phase 2", WordCount.class, SubscriberMapper.class,
			SubscriberReducer.class, SubscriberReducer.class, SubscriberInfo.class, IntWritable.class, 
			intermediatePath, args[1]);
		job.waitForCompletion(true);
	}

	public static Job setupClass(Configuration conf,String jobName,  Class mainClass, Class mapperClass, 
		Class reducerClass, Class combinerClass, Class outputKeyClass, Class outputValueClass,
		String inputPath, String outputPath) throws IOException {
    	Job job = Job.getInstance(conf,jobName );
    	job.setJarByClass(mainClass);
    	job.setMapperClass(mapperClass);
    	job.setCombinerClass(combinerClass);
    	job.setReducerClass(reducerClass);
    	job.setOutputKeyClass(outputKeyClass);
    	job.setOutputValueClass(outputValueClass);
    	FileInputFormat.addInputPath(job, new Path(inputPath));
    	FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	
}


