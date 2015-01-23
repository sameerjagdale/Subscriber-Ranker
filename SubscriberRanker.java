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

public class SubscriberRanker {
	final static  String intermediatePath1 = "intermediate1";	
	final static  String intermediatePath2 = "intermediate2";	
	public static boolean DEBUG = false;

	public static void setDebugFlag(boolean flag) {
		DEBUG = flag;
	}
	
	public static boolean getDebugFlag(boolean flag) {
		return DEBUG;
	}

	public static void main(String[] args) throws Exception {
		execPhase1(args);
		execPhase2(args);	
		execPhase3(args);	
  	}

	public static void execPhase1(String args[]) throws Exception{
		Job job = setupClass(new Configuration(), "Phase 1", SubscriberRanker.class, Phase1Mapper.class,
			Phase1Reducer.class, Phase1Reducer.class, SubscriberAccessInfo.class, IntWritable.class, 
			args[0], intermediatePath1);
		job.waitForCompletion(true);
	}

	public static void execPhase2(String args[]) throws Exception{
		Job job = setupClass(new Configuration(), "Phase 2", SubscriberRanker.class, Phase2Mapper.class,
			Phase2Reducer.class, Phase2Reducer.class, SubscriberInfo.class, IntWritable.class, 
			intermediatePath1, intermediatePath2);
		job.waitForCompletion(true);
	}

	public static void execPhase3(String args[]) throws Exception{
		Job job = setupClass(new Configuration(), "Phase 3", SubscriberRanker.class, Phase3Mapper.class,
			Phase3Reducer.class, Phase3Reducer.class, Text.class, Text.class, 
			intermediatePath2, args[1]);
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


