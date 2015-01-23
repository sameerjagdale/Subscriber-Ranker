package main; 
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
import phase1.*;
import phase2.*;
import phase3.*;
/** The main class of the program. The class sets up the map and reduce jobs for the three 
 * phases. Also defines the folders to which the intermediate results map to. 
 * @author Sameer Jagdale
 */
public class SubscriberRanker {
/** Output folder for the results of the first map reduce operation. 
 */
	final static  String intermediatePath1 = "intermediate1";	
/** Output folder for the results of the second map reduce operation. 
 */
	final static  String intermediatePath2 = "intermediate2";	
/** The DEBUG flag is used to define whether debug mode is one, in which case additional information 
 * can be printed out. 
 */
	public static boolean DEBUG = false;
/** A setter method for the debug flag. 
 * @param flag boolean value to which the debug flag is set. 
 */
	public static void setDebugFlag(boolean flag) {
		DEBUG = flag;
	}
/** A getter method for the debug flag. 
 *	@return returns the DEBUG variable.
 */
	public static boolean getDebugFlag() {
		return DEBUG;
	}

/** Method to setup and execute the first map reduce phase of the program.
 * @param args a string array which contains input and output folders. Currently these values are passed from
 * the command line. 
 */
	public static void execPhase1(String args[]) throws Exception{
		Job job = setupClass(new Configuration(), "Phase 1", SubscriberRanker.class, Phase1Mapper.class,
			Phase1Reducer.class, Phase1Reducer.class, SubscriberAccessInfo.class, IntWritable.class, 
			args[0], intermediatePath1);
		job.waitForCompletion(true);
	}
/** Method to setup and execute the second map reduce phase of the program. 
 * @param args a string array which contains input and output folders. Currently these values are passed from
 * the command line. 
 */
	public static void execPhase2(String args[]) throws Exception{
		Job job = setupClass(new Configuration(), "Phase 2", SubscriberRanker.class, Phase2Mapper.class,
			Phase2Reducer.class, Phase2Reducer.class, SubscriberInfo.class, IntWritable.class, 
			intermediatePath1, intermediatePath2);
		job.waitForCompletion(true);
	}

/** Method to setup and execute the third map reduce phase of the program. 
 * @param args a string array which contains input and output folders. Currently these values are passed from
 * the command line. 
 */
	public static void execPhase3(String args[]) throws Exception{
		Job job = setupClass(new Configuration(), "Phase 3", SubscriberRanker.class, Phase3Mapper.class,
			Phase3Reducer.class, Phase3Reducer.class, Text.class, Text.class, 
			intermediatePath2, args[1]);
		job.waitForCompletion(true);
	}
/** A generic method to setup a hadoop job. Reduces duplicate code since in the current case three different jobs 
 * need to be set up. 
 * @param conf an object of class Configuration. 
 * @param jobName defines the name of the job to be set up. 
 * @param mainClass defines the main class of the job. In this case it is SubscriberRanker. 
 * @param mapperClass defines the class which implement the map method. 
 * @param combinerClass defines the combiner class to be used.
 * @param reducerClass defines the class implementing the reduce method. 
 * @param outputKeyClass defines the type of the output key for the map-reduce operation. 
 * @param outputValueClass defines the type of the output value for the map-reduce operation. 
 */
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
/** Main method of the class. Makes calls to the execPhase* methods to execute different phases of the 
 * program. 
 */
	public static void main(String[] args) throws Exception {
		execPhase1(args);
		execPhase2(args);	
		execPhase3(args);	
  	}
	
}
