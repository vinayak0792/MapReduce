
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


	public class yelpq2
		{
			public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>
				{
				private static TreeMap<Float, String> topTen = new TreeMap<Float, String>();
	
					private final static FloatWritable one = new FloatWritable();
					private Text word = new Text(); // type of output key
					public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
					{
						String[] mydata = value.toString().trim().split("::");
						word.set(mydata[2]);
						one.set(Float.parseFloat(mydata[3]));
						context.write(word,one);
					}
				}
			public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> 
			{
				private  HashMap <Float, String> topTen = new HashMap<Float, String>(); //map to store all the values after calculating the average. 
			//	private FloatWritable result = new FloatWritable();
				public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException 
				{
					float sum = 0; // initialize the sum for each keyword
					int count = 0;
					for (FloatWritable val : values) 
					{
						sum += val.get();
						count = count+1;
						
					}
					float res = sum/count;
					topTen.put( res, key.toString());
					//result.set(res);
					//context.write(key, result); 
					
				}
				
				public void cleanup(Context context)throws IOException, InterruptedException {
					
					TreeMap<Float, String> sortedTopTen = new TreeMap<Float, String>(topTen);
					NavigableMap<Float, String> desc =   sortedTopTen.descendingMap();
				
				    int counter = 0;
					//int i = 0;
					
					
						for (float value : desc.keySet()) {
		                if (counter++ == 10) {
		                    break;
		                }
		                context.write(new Text(desc.get(value)), new FloatWritable(value));
		            }
					//sortedTopTen =  ;
					
				}
				
			}
			
			
			
			
			
			// Driver program
			public static void main(String[] args) throws Exception 
			{
				Configuration conf = new Configuration();
				String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
				// get all args
				if (otherArgs.length != 3) 
				{
					for (int i = 0; i < otherArgs.length; i++ )
					{
						System.out.println(otherArgs[i]+" "+i);
					}
					System.err.println("Usage: WordCount <in> <out>");
					System.exit(2);
				}
				// create a job with name "wordcount"
				Job job = new Job(conf, "wordcount");
				job.setJarByClass(yelpq2.class);
				job.setMapperClass(Map.class);
				job.setReducerClass(Reduce.class);
				//job.setNumReduceTasks(0);
				// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
				// set output key type
				job.setOutputKeyClass(Text.class);
				// set output value type
				job.setOutputValueClass(FloatWritable.class);
				//set the HDFS path of the input data
				FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
				// set the HDFS path for the output
				FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
				//Wait till job completion
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
  }