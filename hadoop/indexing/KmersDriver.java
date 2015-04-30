package hadoop.indexing;

import hadoop.HadoopUtil;
import hadoop.KmerGenericWritable;
import hadoop.inputsplit.FastaFileInputFormat;
import hadoop.inputsplit.FastaLineInputFormat;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import distance.DistanceMeasure;
import utility.Constant;

/**
 * Compute k-tuple distances between DNA sequences.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 2.4
 * 
 * Date: February, 3 2015
 */
public class KmersDriver extends Configured implements Tool {

	private String homeHdfs;
	private int splitStrategy;
	private boolean hasCO, hasCount;
	Class<DistanceMeasure>[] distClasses;

	public KmersDriver(String homeHdfs, int splitStrategy, boolean hasCO, boolean hasCount, Class<DistanceMeasure>[] distClasses){
		this.homeHdfs = homeHdfs;
		this.splitStrategy = splitStrategy;
		this.hasCO = hasCO;
		this.hasCount =hasCount;
		this.distClasses = distClasses;
		
	}
	
	public Class<DistanceMeasure>[] getDistances() {
		return distClasses;
	}

	
	private static void setDistances(Configuration conf, Class<DistanceMeasure>[] distClasses){

		conf.set("NumDistances", ""+distClasses.length);

		for(int i=0; i<distClasses.length; i++){
			conf.setClass("Distance"+(i+1), distClasses[i], DistanceMeasure.class);
		}

	}

	public void start() throws Exception {	
		int res = ToolRunner.run(this, null);
		System.out.println("Esito: "+ res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		setDistances(conf, this.getDistances());

		//conf.set("K", "");
		conf.set("HDFS_HOME_DIR", homeHdfs);
		conf.set("hasCO",hasCO+"");
		conf.set("hasCount",hasCount+"");
		Job job = Job.getInstance(conf, "kmers");

		FileSystem fs = FileSystem.get(conf);

		//		if(homeHdfs==null || homeHdfs.equals(""))
		//			homeHdfs = fs.getHomeDirectory().toString()+"/"; 

		Path inputPath = new Path(homeHdfs+Constant.HDFS_INPUT_DIR);
		Path outputPath = new Path(homeHdfs+Constant.HDFS_OUTPUT_DIR_I); 

		HadoopUtil.delete(fs, homeHdfs+Constant.HDFS_OUTPUT_DIR);

//		//Eventuale copia input sull'HDFS
//		if(Constant.COPY_INPUT_ON_HDFS)
//			copyInputOnHdfs(fs, inputPath);
		

		job.addCacheFile(new Path(homeHdfs+Constant.HDFS_PATTERNS_FILE_HDFS).toUri()); 

		FileInputFormat.setInputPaths(job, inputPath);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		MultipleOutputs.addNamedOutput(job, Constant.TEXT, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Constant.SEQ, SequenceFileOutputFormat.class, Text.class, KmerGenericWritable.class);
		MultipleOutputs.addNamedOutput(job, Constant.SEQ2, SequenceFileOutputFormat.class, Text.class, KmerGenericWritable.class);

		
		job.setJarByClass(this.getClass());

		if(splitStrategy==1)
			job.setInputFormatClass(FastaFileInputFormat.class);
		else
			job.setInputFormatClass(FastaLineInputFormat.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KmerGenericWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KmerGenericWritable.class);

		if(Constant.SET_REDUCE)
			job.setNumReduceTasks(Constant.NUM_REDUCER_JOB_1);

		job.setMapperClass(KmersMapper.class);
		job.setReducerClass(KmersReducer.class);

		int res = job.waitForCompletion(true) ? 0 : 1; // 0 == OK; 1 == ERRORE (KO);

		HadoopUtil.deleteAndCopyMerge(fs, new Path(homeHdfs+Constant.HDFS_OUTPUT_DIR_I), new Path(homeHdfs+Constant.HDFS_OUTPUT_DIR_CHAR_COUNT),conf);
		
		return res;
	}
	
//	private void copyInputOnHdfs(FileSystem fs, Path hdfsInputPath) throws Exception{
//
//		//Copia dell'input file/s
//		Path localPath = new Path(localInputFiles);		
//		HadoopUtil.delete(fs, hdfsInputPath); //cancello qualsiasi cosa nella directory di input.
//		//fs.mkdirs(hdfsInputPath); //creo la nuova directory di input.
//		fs.copyFromLocalFile(localPath, hdfsInputPath);
//
//		//Copia del file di patterns
//		localPath = new Path(localPatternsFile);
//		Path hdfsPatterns = new Path(homeHdfs+Constant.HDFS_PATTERNS_FILE_HDFS);
//		HadoopUtil.delete(fs, hdfsPatterns); 
//		fs.copyFromLocalFile(localPath, hdfsPatterns);
//
//	}

} 

