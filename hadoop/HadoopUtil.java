package hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.ReflectionUtils;

import utility.Constant;
import distance.DistanceMeasure;


/**
 * Util methods.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.7
 * 
 * Date: February, 3 2015
 */
public class HadoopUtil {

	public static List<DistanceMeasure> readDistances(Configuration conf) {//Usato in Hadoop

		int n = Integer.parseInt(conf.get("NumDistances"));

		List<DistanceMeasure> dists = new ArrayList<DistanceMeasure>(); 

		for(int i=1; i<=n; i++){

			String className = conf.getClass("Distance"+i, DistanceMeasure.class).getName();

			DistanceMeasure dm = HadoopUtil.getDistanceMeasure(conf, className);

			if(dm!=null)
				dists.add(dm);
		}

		return dists;
	}


	public static DistanceMeasure getDistanceMeasure(Configuration conf, String className){//Usato in Hadoop

		try {
			Class<?> cls = Class.forName(className);
			//Object obj = cls.newInstance(); 
			Object obj = ReflectionUtils.newInstance(cls, conf);

			if(obj instanceof DistanceMeasure)
				return (DistanceMeasure) obj;

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}   

		return null;
	}

	public static void printDistances(String homeApp, Configuration conf) {

		try{
			NumberFormat formatter = new DecimalFormat("#0.0000000");
			System.out.println("\nDistances:");
			Path pt=new Path(homeApp+Constant.HDFS_DIST_FILE);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line=br.readLine();
			while (line != null){
				//System.out.println(line);
				String[] s = line.split("[ \t]");
				System.out.println("Pattern "+s[2]+" d_"+s[3].replaceAll("distance.", "")+"("+s[0]+","+s[1]+")="+formatter.format(Double.parseDouble(s[4])).replace(",", "."));
				line=br.readLine();
			}
		}catch(Exception e){
			e.printStackTrace();
		}

	}

	public static boolean delete(FileSystem fs, String file) throws Exception{

		return delete(fs, new Path(file)); 

	}


	public static boolean delete(FileSystem fs, Path file) throws Exception{

		if(file==null || file.equals("") || file.equals("/"))
			return false;

		return fs.delete(file, true);

	}


	public static List<String> readPatterns(FileSystem fs, Path pt) {

		Set<String>  res = new HashSet<String>();

		try{
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;

			line=br.readLine();
			new ArrayList<String>(res);
			while (line != null){
				res.add(line);
				line=br.readLine();

			}
		}catch(Exception e){
			e.printStackTrace();
		}

		return new ArrayList<String>(res);
	}


	public static int getMaxkFromPatterns(FileSystem fs, Path pt) {

		int maxk = 0, tmp;

		try{
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;

			line=br.readLine();

			while (line != null){

				tmp = line.length();

				if(tmp > maxk)
					maxk = tmp;

				line=br.readLine();

			}
		}catch(Exception e){
			e.printStackTrace();
		}

		return maxk;
	}

	//Se splitStrategy==1 allora basta mettere tutti i file in un solo file, altrimenti nella seconda strategia di split devo aggregare le lunghezze.
	public static void createIdSeqsFile(FileSystem fs, Configuration conf, String hdfsHomeDir, int splitStrategy) throws Exception{

		Path pathSrc = new Path(hdfsHomeDir+Constant.HDFS_ID_SEQS_DIR);
		Path pathDest = new Path(hdfsHomeDir+Constant.HDFS_ALL_SEQS_IDS);
		HadoopUtil.delete(fs, pathDest);

		if(splitStrategy==1)
			FileUtil.copyMerge(fs, pathSrc, fs, pathDest, false, conf, ""); //Copy all files in a directory to one output file (merge). 
		else
			//Si fa il merge anche dei valori.
			HadoopUtil.copyMergeAll(fs, pathSrc, pathDest);	

		//Cancello la directory di input
		fs.delete(pathSrc, true);
	}

	private static void incrementLenghts(Map<String, Integer> lengthsMap, String idSeq, int length) {

		Integer oldLength = lengthsMap.get(idSeq);

		if(oldLength==null)
			lengthsMap.put(idSeq, length);
		else
			lengthsMap.put(idSeq, oldLength+length);
	}


	public static void copyMergeAll(FileSystem fs, Path pathSrc, Path pathDest) {

		Map<String, Integer> lengthsMap = new HashMap<String, Integer>();

		FileStatus[] status;
		try {
			status = fs.listStatus(pathSrc);

			for(FileStatus f : status)
				addInMap(fs, f.getPath(), lengthsMap);

		} catch (IOException e) {
			e.printStackTrace();
		} 

		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(fs.create(pathDest,true)));

			Iterator<Entry<String, Integer>> it = lengthsMap.entrySet().iterator();

			while(it.hasNext()){
				Entry<String, Integer> e = it.next();

				bw.write(e.getKey()+"\t"+e.getValue()+"\n");
			}
			bw.flush();
			bw.close();

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		lengthsMap.clear();

	}


	public static void deleteAndCopyMerge(FileSystem fs, Path pathSrc, Path pathDest, Configuration conf) {



		FileStatus[] status;
		try {
			status = fs.listStatus(pathSrc);
			
			SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(pathDest), Writer.keyClass(Text.class),
					Writer.valueClass(KmerGenericWritable.class));

			for(FileStatus f : status){
				
				Path seqFilePath = f.getPath();
				if(!seqFilePath.getName().startsWith(Constant.SEQ2))
					continue;
				
				SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));
				

				Text key = new Text();
				KmerGenericWritable val = new KmerGenericWritable();

				while (reader.next(key, val)) {

					writer.append(key, val);

				}
				reader.close();
				
				
				fs.delete(f.getPath(), true);		
			}
			writer.close();
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}



	

}






public static void addInMap(FileSystem fs, Path pathSrc, Map<String, Integer> lengthsMap){

	try{
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathSrc)));
		String line;
		String[] splits;

		line=br.readLine();

		while (line != null){

			splits = line.split("\t");
			try{
				if(splits.length>1)
					incrementLenghts(lengthsMap, splits[0], Integer.parseInt(splits[1]));
			}
			catch(Exception e){
				e.printStackTrace();
			}

			line=br.readLine();
		}

		br.close();

	}catch(Exception e){
		e.printStackTrace();
	}
}



public static void createIdSeqsFileFromHadoopOutput(FileSystem fs, Configuration conf, String hdfsHomeDir) throws Exception{

	Path pathSrc = new Path(hdfsHomeDir+Constant.HDFS_ALL_SEQS_IDS_DIR);
	Path pathDest = new Path(hdfsHomeDir+Constant.HDFS_ALL_SEQS_IDS);
	HadoopUtil.delete(fs, pathDest);
	FileUtil.copyMerge(fs, pathSrc, fs, pathDest, false, conf, ""); //Copy all files in a directory to one output file (merge). 
}


public static void copyInputFilesOnHdfs(FileSystem fs, String localInputFiles, Path hdfsInputPath) throws Exception{

	//Copia dell'input file/s
	Path localPath = new Path(localInputFiles);		
	HadoopUtil.delete(fs, hdfsInputPath); //cancello qualsiasi cosa nella directory di input.
	//fs.mkdirs(hdfsInputPath); //creo la nuova directory di input.
	fs.copyFromLocalFile(localPath, hdfsInputPath);

}


public static void copyPatternsOnHdfs(FileSystem fs, String localPatternsFile,  String homeHdfs) throws Exception{

	//Copia del file di patterns
	Path localPath = new Path(localPatternsFile);
	Path hdfsPatterns = new Path(homeHdfs+Constant.HDFS_PATTERNS_FILE_HDFS);
	HadoopUtil.delete(fs, hdfsPatterns); 
	fs.copyFromLocalFile(localPath, hdfsPatterns);

}

public static void copyProbabilitiesOnHdfs(FileSystem fs, String localProbabilitiesFile,  String homeHdfs) throws Exception{

	//Copia del file di probabilità
	Path localPath = new Path(localProbabilitiesFile);
	Path hdfsProbabilities = new Path(homeHdfs+Constant.PROBABILITIES_PATH);
	HadoopUtil.delete(fs, hdfsProbabilities); 
	fs.copyFromLocalFile(localPath, hdfsProbabilities);

}
}
