package com.ptg.mr;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AirlineCodeShare extends Configured implements Tool {
	public static class RoutesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text airlineId = new Text();
		private Text placeholder = new Text();
		private static final int CODESHARE = 6;
		private static final int AIRLINE_ID = 1;
		private static final String PLACEHOLDER = "x";
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			if(itr[CODESHARE].toString().equalsIgnoreCase("Y") && StringUtils.isNumeric(itr[AIRLINE_ID])) {
				airlineId.set(itr[AIRLINE_ID]);
				placeholder.set(PLACEHOLDER);
				context.write(airlineId, placeholder);
			}
		}
	}
	public static class AirlinesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text airlineId = new Text();
		private Text airlineName = new Text();
		private static final int AIRLINE_ID = 0;
		private static final int AIRLINE_NAME = 1;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			if(StringUtils.isNumeric(itr[AIRLINE_ID])) {
				airlineId.set(itr[AIRLINE_ID]);
				airlineName.set(itr[AIRLINE_NAME]);
				context.write(airlineId, airlineName);
			}
		}
	}
	public static class CodeShareReducer extends Reducer<Text, Text, Text, Text> {
		private Text airlineName = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean hasX = false;
			for(Text val : values) {
				if(val.toString().length() > 1) {
					airlineName.set(val);
				} 
				if (val.toString().equals("x")) {
					hasX = true;
				}
			}
			if(hasX && airlineName.toString().length() > 1) {
				context.write(key, airlineName);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AirlinesCodeShare");
		job.setJarByClass(AirlineCodeShare.class);
		MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, RoutesMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, AirlinesMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setReducerClass(CodeShareReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int ecode = ToolRunner.run(new AirlineCodeShare(), args);
		System.exit(ecode);
	}
}
