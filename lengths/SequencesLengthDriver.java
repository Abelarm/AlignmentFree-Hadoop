package lengths;

import hadoop.HadoopUtil;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import utility.Constant;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: January, 30 2015
 */
public class SequencesLengthDriver extends Configured implements Tool {

	private String hdfsHomeDir;

	public SequencesLengthDriver() {
		this("");
	}

	public SequencesLengthDriver(String hdfsHomeDir) {
		super();
		this.hdfsHomeDir = hdfsHomeDir;
	}

	public void start() throws Exception {
		int res = ToolRunner.run(this, null);
		System.out.println("Esito: "+ res);
	}

	public int run(String[] args) throws Exception {

		Path inputPath = new Path(hdfsHomeDir+Constant.HDFS_ID_SEQS_DIR);
		Path outputPath = new Path(hdfsHomeDir+Constant.HDFS_ALL_SEQS_IDS_DIR);

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "ComputeLength");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(this.getClass());
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(SequencesLengthMapper.class);
		job.setCombinerClass(SequencesLengthReducer.class);
		job.setReducerClass(SequencesLengthReducer.class);

		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);

		int res = job.waitForCompletion(true) ? 0 : 1;
		
		HadoopUtil.createIdSeqsFileFromHadoopOutput(fs, conf, hdfsHomeDir);
		
		return res;
	}

} 

