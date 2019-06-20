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

public class AirportsInIndia {
	public static class TokenizerMapper	extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text airPortId = new Text();
		private Text airPortName = new Text();
		private static final int COUNTRY = 3;
		private static final int AIRPORT_ID = 0;
		private static final int AIRPORT_NAME = 1;


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			if(itr[COUNTRY].toString().equalsIgnoreCase("India")) {
				airPortName.set(itr[AIRPORT_ID] + " " + itr[AIRPORT_NAME]);
				context.write(airPortId, airPortName);
			}
		}
	}

	public static class AirportsReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(result, val);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Airports");
		job.setJarByClass(AirportsInIndia.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(AirportsReducer.class);
		job.setReducerClass(AirportsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

