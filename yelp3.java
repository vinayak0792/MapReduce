
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
import java.util.Map;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


	public class yelp3
		{
			public static class Map1 extends Mapper<LongWritable, Text, Text, FloatWritable>
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
			public static class Reduce1 extends Reducer<Text,FloatWritable,Text,FloatWritable> 
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
			
			public static class Map2 extends Mapper<LongWritable ,Text ,Text,Text> 
			{
				public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException{
					
					String data = value.toString().trim();
					String[] content = data.split("\t");
					
					context.write(new Text(content[0].trim()), new Text("Map2 \t"+content[1]));
				}
			}
			
			public static class Map3 extends Mapper<LongWritable, Text, Text, Text>
			{
				public void map(LongWritable key, Text value,Context context)throws IOException , InterruptedException{
					
					String data = value.toString();
					String[] content = data.split("::");
				//	String joinKey = content[0];
					context.write(new Text(content[0].trim()), new Text("Map3 \t"+content[1] +"\t"+content[2]));
					
				}
			}
			
			public static class Reduce2 extends Reducer<Text, Text , Text , Text>
			{
			private	HashMap<String, String> topTen = new HashMap<String, String>();
			private	HashMap<String, String> business_details = new HashMap< String, String>();
				public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
				{
					for(Text data : value){
					String values = data.toString();
					
					if(values.startsWith("Map2")){
						topTen.put(key.toString(), values.substring(4).trim());
						
					}
					else if (values.startsWith("Map3"))
					{
						business_details.put(key.toString(), values.substring(4).trim());
					}
				
					}
					
					
				}
				
				public void cleanup(Context context)throws IOException, InterruptedException{
					
					String key1;
					String key2;
					String value1;
					String value2;
					for(Map.Entry<String, String> i : topTen.entrySet()){
						key1 = i.getKey();
						value1 = i.getValue().trim();
						for(Map.Entry<String, String> j: business_details.entrySet()){
							key2 = j.getKey();
							if(key1.equals(key2))
							{	
								value2 = j.getValue().trim();
								context.write(new Text(key1), new Text(value2+"\t"+value1));
							}
						}
					}
			
				}
			}
			
			
			
			
			// Driver program
			public static void main(String[] args) throws Exception 
			{
				Configuration conf = new Configuration();
				String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
				// get all args
				if (otherArgs.length != 5) 
				{
					for (int i = 0; i < otherArgs.length; i++ )
					{
						System.out.println(otherArgs[i]+" "+i);
					}
					System.err.println("Usage: WordCount <in> <out>");
					System.exit(2);
				}
				// Creating 1st job to fetch the TopTen Business's based on average ratings. 
				Job job1 = new Job(conf, " MPR1 ");
				job1.setJarByClass(yelp3.class);
				job1.setMapperClass(Map1.class);
				job1.setReducerClass(Reduce1.class);
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(FloatWritable.class);
				FileInputFormat.addInputPath(job1, new Path(otherArgs[1])); //Setting the inputPath for Job1.
				FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2])); //Setting intermidiate outputPath for Job1. 
				boolean job1completed = job1.waitForCompletion(true);
				
				if(job1completed)
				{
					Job job2 = new Job(conf, "MRP2");
					job2.setJarByClass(yelp3.class);
					//FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
					job2.setOutputKeyClass(Text.class);
					job2.setOutputValueClass(Text.class);
//					job2.setInputFormatClass(TextInputFormat.class);
//					job2.setOutputFormatClass(TextOutputFormat.class);
					MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, Map2.class);
					MultipleInputs.addInputPath(job2, new Path(otherArgs[3]), TextInputFormat.class, Map3.class);
					job2.setReducerClass(Reduce2.class);
					FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
					job2.waitForCompletion(true);
				}
				
				
				
				
			}
  }