package hadoop.piarwisesimilarity;


import hadoop.DistanceDoubleWritable;
import hadoop.DistanceGenericWritable;
import hadoop.HadoopUtil;
import hadoop.IdSeqsWritable;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import distance.DistanceMeasure;
import distance.d2.EstimatedProb;
import distance.d2.FixedProb;
import utility.Constant;
import utility.Util;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 2.6
 * 
 * Date: February, 1 2015
 */
public class PairwiseSimilarityDriver extends Configured implements Tool {

	private Class<DistanceMeasure>[] distances;
	private String hdfsHomeDir;
	private String outputLocalDir;
	private int splitStrategy;

	public PairwiseSimilarityDriver(){
		this(null, null, null, 0);
	}

	public PairwiseSimilarityDriver(String hdfsHomeDir, Class<DistanceMeasure>[] distanceClasses, String outputLocalDir, int splitStrategy){
		super();
		this.hdfsHomeDir = hdfsHomeDir;
		this.distances = distanceClasses;
		this.outputLocalDir = outputLocalDir;
		this.splitStrategy = splitStrategy;
	}

	public Class<DistanceMeasure>[] getDistances() {
		return distances;
	}

	public void setDistances(Class<DistanceMeasure>[] distances) {
		this.distances = distances;
	}

	public void start() throws Exception {
		int res = ToolRunner.run(this, null);
		System.out.println("Esito: "+ res);

	}

	private static void setDistances(Configuration conf, Class<DistanceMeasure>[] distClasses){

		conf.set("NumDistances", ""+distClasses.length);

		for(int i=0; i<distClasses.length; i++){
			conf.setClass("Distance"+(i+1), distClasses[i], DistanceMeasure.class);
		}

	}


	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		setDistances(conf, this.getDistances());

		Job job = Job.getInstance(conf, "Pairwise similarity");

	
		FileSystem fs = FileSystem.get(conf);

//		if(hdfsHomeDir==null || hdfsHomeDir.equals(""))
//			hdfsHomeDir = fs.getHomeDirectory().toString()+"/"; 
		
		Path inputPath = new Path(hdfsHomeDir+Constant.HDFS_OUTPUT_DIR_I);
		Path outputPath = new Path(hdfsHomeDir+Constant.HDFS_OUTPUT_DIR_II);
		
	
		
		
		
		//FileInputFormat.setInputPaths(job, inputPath);
		SequenceFileInputFormat.addInputPath(job,inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		MultipleOutputs.addNamedOutput(job, Constant.TEXT, TextOutputFormat.class, Text.class, DoubleWritable.class);
		
		deleteLocalOutputDir(conf);

		deleteUnimportantFiles(fs, inputPath);
		
		HadoopUtil.createIdSeqsFile(fs, conf, hdfsHomeDir, splitStrategy); 

		job.addCacheFile(new Path(hdfsHomeDir+Constant.HDFS_ALL_SEQS_IDS).toUri());
		
		boolean fixed=false;
		boolean estimated=false;
		for(Class<DistanceMeasure> c : distances){
			if(fixed && estimated)
				break;
			if(c.newInstance() instanceof FixedProb && !fixed){
				fixed=true;
			}else{
				if(c.newInstance() instanceof EstimatedProb && !estimated){
					estimated=true;
				}
			}
		}
		
		if(estimated)
			job.addCacheFile(new Path(hdfsHomeDir+Constant.HDFS_OUTPUT_DIR_CHAR_COUNT).toUri());
		if(fixed)
			job.addCacheFile(new Path(hdfsHomeDir+Constant.PROBABILITIES_PATH).toUri());	//TODO da testare
		
		
		
		
		job.setJarByClass(this.getClass());
		

		if(Constant.SET_REDUCE)
			job.setNumReduceTasks(Constant.NUM_REDUCER_JOB_2);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.setMapOutputKeyClass(IdSeqsWritable.class);
		job.setMapOutputValueClass(DistanceDoubleWritable.class);

		job.setOutputKeyClass(IdSeqsWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(PairwiseSimilarityMapper.class);
		job.setCombinerClass(PairwiseSimilarityCombiner.class);
		job.setReducerClass(PairwiseSimilarityReducer.class);

		int res = job.waitForCompletion(true) ? 0 : 1; // 0 == OK; 1 == ERRORE (KO) 

		if(res==0 && Constant.MERGE_DISTANCES_ON_HDFS)
			mergeOutputFileonHdfs(fs, conf);
		
		if(res==0 && Constant.MERGE_DISTANCE_ON_LOCAL_FS){
			mergeOutputFileOnLocalFs(fs, conf);
			
			if(Constant.HADOOP_PRINT_DISTANCES)
				Util.printDistances(outputLocalDir+Constant.LOCAL_OUTPUT_FILE_HADOOP); //Legge da local FS.
			
		}

//		if(res==0 && Constant.PRINT_DISTANCES)
//			HadoopUtil.printDistances(hdfsHomeDir, conf); //Legge da HDFS
			
		return res;
	}

	private void mergeOutputFileonHdfs(FileSystem fs, Configuration conf) throws Exception{

		Path pathSrc = new Path(hdfsHomeDir+Constant.HDFS_OUTPUT_DIR_II); 
		Path pathDest = new Path(hdfsHomeDir+Constant.HDFS_DIST_FILE);
		HadoopUtil.delete(fs, pathDest);
		FileUtil.copyMerge(fs, pathSrc, fs, pathDest, false, conf, ""); //Copy all files in a directory to one output file (merge). 

	}
	
	private void deleteLocalOutputDir(Configuration conf) throws Exception{

		if(outputLocalDir==null || outputLocalDir.equals("")==true || outputLocalDir.equals("/"))
			return;
		
		Path path = new Path(outputLocalDir); 
		FileSystem localFs = FileSystem.getLocal(conf);
		HadoopUtil.delete(localFs, path);
	}
	
	private void mergeOutputFileOnLocalFs(FileSystem hdfs, Configuration conf) throws Exception{

		Path pathSrc = new Path(hdfsHomeDir+Constant.HDFS_OUTPUT_DIR_II); 
		Path pathDest = new Path(outputLocalDir+Constant.LOCAL_OUTPUT_FILE_HADOOP); 
		FileSystem localFs = FileSystem.getLocal(conf);
		HadoopUtil.delete(localFs, pathDest);
		localFs.mkdirs(new Path(outputLocalDir));
		FileUtil.copyMerge(hdfs, pathSrc, localFs, pathDest, false, conf, ""); //Copy all files in a directory to one output file (merge). 
		
		hdfs.copyToLocalFile(new Path(hdfsHomeDir+Constant.HDFS_ALL_SEQS_IDS), new Path(outputLocalDir+Constant.LOCAL_OUTPUT_IDS_HADOOP));
	}

	private void deleteUnimportantFiles(FileSystem fs, Path inputPath) throws Exception{
		//cancella i file non importanti dalla directory di input. 
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(inputPath, true);	
		while(files.hasNext()){
			LocatedFileStatus f  = files.next();

			if(!f.getPath().getName().startsWith(Constant.SEQ))
				HadoopUtil.delete(fs, f.getPath());
		}

	}

} 

