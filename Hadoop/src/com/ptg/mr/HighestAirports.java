package com.ptg.mr;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HighestAirports {
	public static class AirportsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private Text countryName = new Text();
		private LongWritable altitude = new LongWritable();
		private static final int ALTITUDE = 8;
		private static final int COUNTRY_NAME = 3;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(",");
			if(StringUtils.isNumeric(itr[ALTITUDE])) {
				altitude.set(Integer.parseInt(itr[ALTITUDE]));
				countryName.set(itr[COUNTRY_NAME]);
				context.write(altitude, countryName);
			}
		}
	}
	public static class AirportsReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text val : values) {
				context.write(key, val);
			}
		}
	}

	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "HighestAirport");
		    job.setJarByClass(HighestAirports.class);
		    job.setMapperClass(AirportsMapper.class);
		    job.setCombinerClass(AirportsReducer.class);
		    job.setReducerClass(AirportsReducer.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
