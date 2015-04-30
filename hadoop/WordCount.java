package hadoop;
import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class WordCount extends Configured implements Tool {

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new WordCount(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "word count");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(this.getClass());
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			String line = value.toString();
//			StringTokenizer tokenizer = new StringTokenizer(line);
//			while (tokenizer.hasMoreTokens()) {
//				word.set(tokenizer.nextToken());
//				context.write(word, one);
//			}
			
			String[] lines = tokenize(value.toString());
			
			for(String token : lines){
				word.set(token);
				context.write(word, one);
			}
		}
		
		private String[] tokenize(String text) {
			  text = text.toLowerCase();
			  text = text.replace("'","");
			  text = text.replaceAll("[\\s\\W]+", " ").trim();
			  return text.split(" ");
			}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

} 

