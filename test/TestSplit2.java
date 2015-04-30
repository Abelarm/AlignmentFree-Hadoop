package test;

import hadoop.inputsplit.FastaLineInputFormat;
import hadoop.inputsplit.ValueWritable;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import utility.Constant;


public class TestSplit2 extends Configured implements Tool {

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new TestSplit2(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Path inputPath = new Path("/home/user/Scrivania/big.fasta");
		//Path inputPath = new Path("./data/example/prova2.fasta");
		Path outputPath = new Path("./data/example/Output");

		
//		inputPath = new Path("INPUT");
//		outputPath = new Path("OUTPUT");
		
		
		Configuration conf = getConf();
		
		String hdfsHomeDir = "/home/user/Scrivania/workspace/AlignmentFreeHadoop/data/HDFS/HAFS/";
		conf.set("HDFS_HOME_DIR", hdfsHomeDir);
		
		Job job = Job.getInstance(conf, "word count");
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(this.getClass());
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(FastaLineInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		//job.setReducerClass(Reduce.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, ValueWritable, Text, NullWritable> {
		private final IntWritable one = new IntWritable(1);
		private Text wordKey = new Text();
		private Text wordValue = new Text();
		private boolean first = true;
		private NullWritable nw = NullWritable.get();
		private MultipleOutputs<Text, NullWritable> mos;
		private int k = 3;
		
		@Override
		protected void setup(
				Mapper<Text, ValueWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			super.setup(context);

			mos = new MultipleOutputs<Text, NullWritable>(context);
			
			first = true;

			
		}

		@Override
		public void map(Text key, ValueWritable value, Context context) throws IOException, InterruptedException {
			
//			if(Constant.DEBUG_MODE)
//				System.err.println(key + " "+value.toString());
//			
//			int size;
//			String genome = value.getCurrLine().toString();
//			
//			if(value.getNextLine()==null)// Ultima linea del file di input.
//				size = value.getCurrLine().getLength() - k + 1;
//			else{
//				size = value.getCurrLine().getLength();
//				genome+=value.getNextLine().toString();
//			}	
//			
//			for(int i=0; i<size; i++){
//				String kmer = genome.substring(i, i+k);
//				System.out.println(kmer);
//			}
			
			wordKey.set(key);
			
			wordValue.set(value.getCurrLine());
			mos.write(wordValue, nw, "File1");
			
			if(first && key.toString().contains(".0")){
				first=false;
				wordValue.set(value.getCurrLine());
				mos.write(wordValue, nw, "File2");
			}
			
			if(value.getNextLine()!=null){
				wordValue.set(value.getNextLine());
				mos.write(wordValue, nw, "File2");
			}

		}
		
		@Override
		protected void cleanup(
				Mapper<Text, ValueWritable, Text, NullWritable>.Context context)
						throws IOException, InterruptedException {
			super.cleanup(context);
			mos.close();
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

		private final static NullWritable nw = NullWritable.get();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//int sum = 0;
			for (Text value : values) {
				//sum += value.get();
				context.write(value, nw);
			}

			//context.write(key, new IntWritable(sum));
		}
	}

} 

