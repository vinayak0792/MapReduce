import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.io.*;
import java.util.HashSet;
import java.io.InputStreamReader;
import java.lang.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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

	public class yelpq1
		{
			public static class Map extends Mapper<LongWritable, Text, Text, Text>
				{	
				   
					
					public  void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
					{
						Text categories = new Text();
						Text keys = new Text("PaloAlto");
						String record = value.toString();
						String[] contents = record.split("::");
						String add = contents[1].toLowerCase();
						if(add.contains("palo alto"))
						{
							categories.set(contents[2].trim());
							context.write(keys, categories);
						}

					}
				}
			public static class Reduce extends Reducer<Text,Text,Text,NullWritable> 
			{
				//private Text dummy = new Text("");
				private static HashSet<String> list = new HashSet<>();
				private HashSet<String> categories = new HashSet<>();
				public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
				{	
					//String value;
				
					for(Text word : values)
					{
						String cats = word.toString();
						int openIndex = cats.indexOf("(");
						int closeIndex = cats.indexOf(")");
						String[] vals = cats.substring(openIndex+1, closeIndex).trim().split(",");
						for (String x : vals)
						{	
							if(!list.contains(x))
							{
								list.add(x);
							}
							else
							{
							//	result.set(x);
								categories.add(x.trim());
//								
//								context.write(result, dummy);
							}
							
							
						}
						
					}
					
				}
				
				public void cleanup(Context context)throws IOException,InterruptedException{
					
					NullWritable nullOb = NullWritable.get();

					//String value;
					for(String value : categories)
					{
						context.write(new Text(value.trim()), nullOb);
					}
					
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
				Job job = new Job(conf, "yelp");
				job.setJarByClass(yelpq1.class);
				job.setMapperClass(Map.class);
				job.setReducerClass(Reduce.class);
				// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
				// set output key type
				job.setOutputKeyClass(Text.class);
				// set output value type
				job.setOutputValueClass(Text.class);
				//set the HDFS path of the input data
				FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
				// set the HDFS path for the output
				FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
				//Wait till job completion
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
  }