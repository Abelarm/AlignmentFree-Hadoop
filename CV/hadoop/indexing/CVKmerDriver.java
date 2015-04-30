package CV.hadoop.indexing;


import hadoop.HadoopUtil;
import hadoop.inputsplit.FastaFileInputFormat;
import hadoop.inputsplit.FastaLineInputFormat;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import CV.hadoop.ArrayKmer4CountsWritable;
import CV.hadoop.Kmer4CountsWritable;
import utility.Constant;

/**
 * Compute k, k-1 and k-2-tuple counts of a sequence, according to CV.
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 1.2
 * 
 * Date: February, 16 2015
 */
public class CVKmerDriver extends Configured implements Tool {


	private String localInputFiles;
	private String localPatternsFile;
	private String homeHdfs;
	private int splitStrategy;

	public CVKmerDriver(String localInputFiles, String localPatternsFile, String homeHdfs, int splitStrategy){
		this.localInputFiles = localInputFiles;
		this.localPatternsFile = localPatternsFile;
		this.homeHdfs = homeHdfs;
		this.splitStrategy = splitStrategy;
	}

	public void start() throws Exception {	
		int res = ToolRunner.run(this, null);
		System.out.println("Esito: "+ res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		//conf.set("K", "");
		conf.set("HDFS_HOME_DIR", homeHdfs);
		Job job = Job.getInstance(conf, "kmers");

		FileSystem fs = FileSystem.get(conf);

		//		if(homeHdfs==null || homeHdfs.equals(""))
		//			homeHdfs = fs.getHomeDirectory().toString()+"/"; 

		Path inputPath = new Path(homeHdfs+Constant.HDFS_INPUT_DIR);
		Path outputPath = new Path(homeHdfs+Constant.HDFS_OUTPUT_DIR_I); 

		HadoopUtil.delete(fs, homeHdfs+Constant.HDFS_OUTPUT_DIR);

		//Eventuale copia input sull'HDFS
		if(Constant.COPY_INPUT_ON_HDFS)
			copyInputOnHdfs(fs, inputPath);

		job.addCacheFile(new Path(homeHdfs+Constant.HDFS_PATTERNS_FILE_HDFS).toUri()); 

		FileInputFormat.setInputPaths(job, inputPath);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		MultipleOutputs.addNamedOutput(job, Constant.TEXT, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constant.SEQ, SequenceFileOutputFormat.class, Text.class, ArrayKmer4CountsWritable.class);

		job.setJarByClass(this.getClass());

		if(splitStrategy==1)
			job.setInputFormatClass(FastaFileInputFormat.class);
		else
			job.setInputFormatClass(FastaLineInputFormat.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Kmer4CountsWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayKmer4CountsWritable.class);

		if(Constant.SET_REDUCE)
			job.setNumReduceTasks(Constant.NUM_REDUCER_JOB_1);

		job.setMapperClass(CVKmerMapper.class);
		job.setReducerClass(CVKmerReducer.class);

		return job.waitForCompletion(true) ? 0 : 1; // 0 == OK; 1 == ERRORE (KO)
	}

	
	private void copyInputOnHdfs(FileSystem fs, Path hdfsInputPath) throws Exception{

		//Copia dell'input file/s
		Path localPath = new Path(localInputFiles);		
		HadoopUtil.delete(fs, hdfsInputPath); //cancello qualsiasi cosa nella directory di input.
		//fs.mkdirs(hdfsInputPath); //creo la nuova directory di input.
		fs.copyFromLocalFile(localPath, hdfsInputPath);

		//Copia del file di patterns
		localPath = new Path(localPatternsFile);
		Path hdfsPatterns = new Path(homeHdfs+Constant.HDFS_PATTERNS_FILE_HDFS);
		HadoopUtil.delete(fs, hdfsPatterns); 
		fs.copyFromLocalFile(localPath, hdfsPatterns);

	}

} 

