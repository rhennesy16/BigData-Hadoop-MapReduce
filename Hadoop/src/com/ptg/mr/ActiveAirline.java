package com.ptg.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActiveAirline {
	public static class ActiveAirlineMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text country = new Text();
		private Text airline = new Text();
		private static final int COUNTRY = 6;
		private static final int AIRLINE = 1;
		private static final int IATA = 3;
		private static final int ACTIVE_AIRLINE = 7;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			if(itr[COUNTRY].equalsIgnoreCase("United States") && itr[IATA].length() == 2 && itr[ACTIVE_AIRLINE].toString().equalsIgnoreCase("Y")) {
				country.set(itr[COUNTRY]);
				airline.set(itr[AIRLINE]);
				context.write(country, airline);
			}
		}
	}
	public static class ActiveAirlineReducer extends Reducer<LongWritable, Text, Text, Text> {
		public void reducer(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text val : values) {
				context.write(key, val);
			}
		}
	}
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ActiveAirline");
	    job.setJarByClass(ActiveAirline.class);
	    job.setMapperClass(ActiveAirlineMapper.class);
	    job.setCombinerClass(ActiveAirlineReducer.class);
	    job.setReducerClass(ActiveAirlineReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
