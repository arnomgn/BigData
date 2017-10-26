
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class Join {
	// Complete the JoinMapper class. 
	// Definitely, Generic type (LongWritable, Text, Text, Text) must not be modified
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		String[] tableNames = new String[2];
		String first_table_name = null;
		int first_table_join_index;
		String second_table_name = null;
		int second_table_join_index;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			// Don't change setup function
			tableNames = context.getConfiguration().getStrings("table_names");
			first_table_join_index = context.getConfiguration().getInt("first_table_join_index", 0);
			second_table_join_index = context.getConfiguration().getInt("second_table_join_index", 0);
			first_table_name = tableNames[0];
			second_table_name = tableNames[1];
		}
		
		public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
			
		    Text newKey=new Text();
		    
	        List<String> values =  new LinkedList<String>(Arrays.asList(record.toString().split("\\s*,\\s*")));
	        
		    String Nametab=values.get(0).replace("\"", "");
		    
		    
	        if(Nametab.equals(first_table_name)) {
	        	newKey.set(values.get(first_table_join_index).replace("\"", ""));
	        }
	        else if(Nametab.equals(second_table_name)) {
	        	newKey.set(values.get(second_table_join_index).replace("\"", ""));
	        }
	        String newValue = values.toString();
	        
	        context.write(newKey, new Text(newValue));

		}
	}
	

	// Don't change (key, value) types
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

		//protected void setup(Context context) throws IOException, InterruptedException {
			// Similar to Mapper Class
			

		//}
		@Override
		public void reduce(Text order_id, Iterable<Text> records, Context context) throws IOException, InterruptedException {
			// Implement reduce function
			// You can see form of new (key, value) pair in sample output file on server.
			// You can use Array or List or other Data structure for 'cache'.
			
			 String temp = new String();
			 List<String> temp2 = new ArrayList<String>();
			 int i=0; 

			 for(Text value:records)
			 {
				 System.out.println(value.toString());
				 
			  if(value.toString().contains("order")) {
				  temp = value.toString().replace("[","").replace("]", "");
				  
			  }
			  else {
				  temp2.add(value.toString().replace("[", "," ).replace("]", ""));
				  
			  }
			  
			 }
			 for(i=0;i<temp2.size();i++) {
				 context.write(order_id, new Text(temp + temp2.get(i)));
			}

		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Table Join");

		job.setJarByClass(Join.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	    
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setStrings("table_names", args[2]);
		job.getConfiguration().setInt("first_table_join_index", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("second_table_join_index", Integer.parseInt(args[4]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}