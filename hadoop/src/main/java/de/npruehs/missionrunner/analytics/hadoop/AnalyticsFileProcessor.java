package de.npruehs.missionrunner.analytics.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyticsFileProcessor {
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		
		private Text eventId = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			
			while (tokenizer.hasMoreTokens()) {
				// Add event.
				String nextEventToken = tokenizer.nextToken();
				eventId.set(nextEventToken);
				context.write(eventId, one);
				
				// Add subevents.
				final char seperator = ':';
				int seperatorIndex;
				while ((seperatorIndex = nextEventToken.lastIndexOf(seperator)) >= 0) {
					nextEventToken = nextEventToken.substring(0, seperatorIndex);
					
					eventId.set(nextEventToken);
					context.write(eventId, one);
				}
			}
		}
	}

	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}

	public boolean process(String fileName) throws Exception {
		String inputPath = "hdfs://localhost:9000/user/npruehs/input/" + fileName;
		String outputPath = "hdfs://localhost:9000/user/npruehs/output/" + fileName;

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "missionrunner");
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		return job.waitForCompletion(true);
	}
}
