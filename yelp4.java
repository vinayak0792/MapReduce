package yelp4;

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
import org.apache.hadoop.filecache.DistributedCache;
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

public class yelp4 {
	@SuppressWarnings("deprecation")
	public static class InMemoMap extends Mapper<LongWritable, Text, Text, Text>
	{
		HashSet<String> cache_set = new HashSet<>(); 
//		int count = 1;
//		int countt = 1;
		
		public void setup(Context context)throws IOException,InterruptedException {
			
			
			Path[] cache_file = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(cache_file.length > 0)
			{
				//prepFile(cache_file[0].toString());
				String address;
				String line;
				BufferedReader read_file = new BufferedReader(new FileReader(cache_file[0].toString()));
				while((line = read_file.readLine() )!= null)
				{
					
					String[] data = line.toString().split("::");
					if(data[1].toLowerCase().contains("stanford"))
					{
					cache_set.add(data[0].trim());
					}
					
				}
				//System.out.println("Cache set **************"+cache_set);
				read_file.close();
				
				
				
			}
			
			
		}
		
//		private void prepFile(String cache_file)
//		{
//			
//		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
			
			String line = value.toString();
			
			String[] content = line.split("::");
			if(cache_set.contains(content[2]))
			{	
				//count++;
				
				context.write(new Text(content[1]), new Text(content[3]));
			}
			//System.out.println(count+"\t"+countt);
		}
		
	}
	

	@SuppressWarnings("deprecation")
	public static void main(String[] args)throws Exception{
		
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf, "memory_join");
		job.setJarByClass(yelp4.class);
		job.setMapperClass(InMemoMap.class);
		job.setNumReduceTasks(0);
		
		DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(), job.getConfiguration());
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1])); //Setting the inputPath for Job1.
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}
