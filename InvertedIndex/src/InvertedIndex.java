import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private final static Text document = new Text();
		
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
	
			String line=value.toString();
			String s  = line.replaceAll("\\.", " ");
			StringTokenizer itr = new StringTokenizer(s);
			while(itr.hasMoreTokens()) {
				
				context.write(new Text(itr.nextToken().toLowerCase()),  new Text(fileName));
			}
		}
	}
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text >{
		
		
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder stringBuilder = new StringBuilder();
			for (Text value : values) {
				stringBuilder.append(value);
				
				if (values.iterator().hasNext()) {
					stringBuilder.append(" ");
				}
				
			}
			
			context.write(key, new Text(stringBuilder.toString()));
			
		      
		     
		}
	}
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Usecase1");
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setJarByClass(InvertedIndex.class);
		    job.setMapperClass(InvertedIndexMapper.class);
		    job.setReducerClass(InvertedIndexReducer.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
