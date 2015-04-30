package hadoop;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

import utility.Constant;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.0
 * 
 * Date: February, 3 2015
 */
public class CopyProbabilities extends Configured implements Tool {

	private String homeHdfs;
	private String localProbFile;

	public CopyProbabilities(String localProbFile, String homeHdfs) {
		super();
		this.homeHdfs = homeHdfs;
		this.localProbFile = localProbFile;
	}

	public void start() throws Exception {	
		int res = ToolRunner.run(this, null);
		System.out.println("Esito: "+ res);
	}

	public int run(String[] args) throws Exception {

		int res = 0; // 0 == OK; 1 == ERRORE (KO)

		Configuration conf = getConf();

		FileSystem fs = FileSystem.get(conf);

		try{
			Path inputPath = new Path(homeHdfs+Constant.HDFS_INPUT_DIR);

			//HadoopUtil.delete(fs, homeHdfs);

			HadoopUtil.copyProbabilitiesOnHdfs(fs, localProbFile, homeHdfs);
		}
		catch(Exception e){
			e.printStackTrace();
			res = 1;
		}

		return res; 
	}


} 

