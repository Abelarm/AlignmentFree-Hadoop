package hadoop.indexing;

import hadoop.HadoopUtil;
import hadoop.KmerCOWritable;
import hadoop.KmerCountWritable;
import hadoop.KmerGenericWritable;
import hadoop.inputsplit.ValueWritable;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import distance.DistanceMeasure;
import distance.d2.ConcatEstimatedProbD2Star;
import distance.d2.EstimatedProb;
import distance.d2.EstimatedProbD2S;
import distance.d2.EstimatedProbD2Star;
import utility.Constant;
import utility.Util;

/**
 * Mapper: Compute k-tuple distances between DNA sequences.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 1.7
 * 
 * Date: February, 15 2015
 */
public class KmersMapper extends Mapper<Text, Writable, Text, Writable> {

	private final KmerGenericWritable myw = new KmerGenericWritable();
	private final Text kword = new Text();
	private final Text id_Seq = new Text();
	private final IntWritable intero = new IntWritable();
	private final Text object = new Text();
	private final KmerCountWritable kcw = new KmerCountWritable();
	private final KmerCOWritable kco = new KmerCOWritable();

	private final HashMap<KmerSeq, Integer> mapCount = new HashMap<KmerSeq, Integer>();	
	private final HashMap<KmerSeq, Text> mapObject = new HashMap<KmerSeq, Text>();	

	private MultipleOutputs<Text, Writable> mos;
	private List<DistanceMeasure> distances; 
	private Counter countErrors;
	private Counter countFunctions;
	private Counter countGenomeStrings;
	private List<String> patterns;
	private int k;
	private String HDFS_HOME_DIR;
	private Text idSequence;
	//private Log LOG = LogFactory.getLog(KmersMapper.class);
	private long length=0;
	private boolean hasCO=false,hasCount=false;

	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);

		distances = HadoopUtil.readDistances(context.getConfiguration());

		mos = new MultipleOutputs<Text, Writable>(context);

		//context.getConfiguration().set("errors", "hasOccured");
		HDFS_HOME_DIR = context.getConfiguration().get("HDFS_HOME_DIR");
		hasCO = Boolean.valueOf(context.getConfiguration().get("hasCO"));
		hasCount = Boolean.valueOf(context.getConfiguration().get("hasCount"));
		

		countErrors = context.getCounter("Kmers Mapper","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("Kmers Mapper","number of functions");
		countFunctions.setValue(0);

		countGenomeStrings = context.getCounter("Kmers Mapper","number of genome strings");
		countGenomeStrings.setValue(0);


		FileSystem fs = FileSystem.get(context.getConfiguration());

		URI[] localPaths = context.getCacheFiles();

		if(localPaths.length>0){
			patterns = HadoopUtil.readPatterns(fs, new Path(localPaths[0]));

			if(Constant.DEBUG_MODE)
				System.out.println("Patterns: "+patterns);
		}
		
		for(int i=0;i<distances.size();i++) 
			if(distances.get(i) instanceof EstimatedProb){
				if(!patterns.contains("1"))
					patterns.add(0, "1");
			}

		//System.out.println("K="+context.getConfiguration().get("K"));
	}


	//	@Override
	//	public void map(Text idSeq, Text allGenome, Context context) throws IOException, InterruptedException {
	//
	//		try{
	//			
	//			countFunctions.increment(1);
	//			
	//			String allStrGenome = allGenome.toString().trim();
	//			
	//			if(allStrGenome.equals("") || !Util.isValidFASTAFormat(allStrGenome))
	//				return;
	//			
	//			countGenomeStrings.increment(1);
	//
	//			if(Constant.DEBUG_MODE){
	//				String filename= ((FileSplit)context.getInputSplit()).getPath().getName();
	//				System.out.println("Filename: "+filename+": <"+idSeq+","+allStrGenome+">");
	//			}
	//			
	//			mos.write(Constant.TEXT, idSeq, allStrGenome.length(), HDFS_HOME_DIR+Constant.HDFS_ID_SEQS_FILE);
	//			
	//			for(String pattern : patterns){
	//				
	//				k = pattern.length();
	//				
	//				/* cycle over the length of String till k-mers of length, k, can still be made */
	//				for(int i = 0; i< (allStrGenome.length()-k+1); i++){
	//					/* output each k-mer of length k, from i to i+k in String*/
	//
	//					String kmer = allStrGenome.substring(i, i+k);
	//
	//					incrementValue(kmer, idSeq.toString(), pattern);
	//
	//				}	
	//			}
	//		}
	//		catch(Exception e){
	//			e.printStackTrace();
	//			countErrors.increment(1);
	//		}
	//	}

	@Override
	public void map(Text idSeq, Writable genome, Context context) throws IOException, InterruptedException {

		countFunctions.increment(1);
		context.progress();
		
		if(genome instanceof Text)
			computeAllGenome(idSeq, (Text) genome, context);
		else if(genome instanceof ValueWritable)
			computePartialGenome(idSeq, (ValueWritable) genome, context);
		
		context.progress();

	}



	public void computeAllGenome(Text idSeq, Text allGenome, Context context) throws IOException, InterruptedException {
		try{
			//int l = distances.size();
			String kmer;
			String allStrGenome = allGenome.toString().trim().toLowerCase();

			if(allStrGenome.equals("") || !Util.isValidFASTAFormat(allStrGenome))
				return;

			countGenomeStrings.increment(1);

			if(Constant.DEBUG_MODE){
				String filename= ((FileSplit)context.getInputSplit()).getPath().getName();
				System.out.println("Filename: "+filename+": <"+idSeq+","+allStrGenome+">");
			}

			
			
			mos.write(Constant.TEXT, idSeq, allStrGenome.length(), HDFS_HOME_DIR+Constant.HDFS_ID_SEQS_FILE);

			
			
			for(String pattern : patterns){

				if(Util.isValidSpacedWordPattern(pattern)){
					k = pattern.length();

					/* cycle over the length of String till k-mers of length, k, can still be made */
					for(int i = 0; i< (allStrGenome.length()-k+1); i++){
						/* output each k-mer of length k, from i to i+k in String*/

						kmer = allStrGenome.substring(i, i+k);

						incrementValue(kmer, idSeq.toString(), pattern);
					}
					
					
				}
				
			}
			
			/* Svuoto man mano la mappa */
			cleanupKmerCO(context);
			cleanupKmerCount(context);
			
		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
	}


	/*
	 * In questo caso ogni file di input è molto grande e contiene una sola sequenza genomica.
	 * Tutte le righe del file sono lunghe Constant.FASTA_MAX_LINE_LENGTH=80 caratteri tranne la prima (id delle sequenza).
	 * L'ultima riga può contenere meno di Constant.FASTA_MAX_LINE_LENGTH.
	 * Nel nostro caso la dimensione del pattern k deve essere minore o uguale a Constant.FASTA_MAX_LINE_LENGTH.
	 */
	public void computePartialGenome(Text idSeq, ValueWritable value, Context context) throws IOException, InterruptedException {

		try{
			int size=0;
			int l=distances.size();
			String genomeStr, kmer, next=null;

			genomeStr = value.getCurrLine().toString().trim().toLowerCase();

			if(value.getNextLine()!=null)
				next = value.getNextLine().toString().trim().toLowerCase();

			if(!Util.isValidFASTAFormat(genomeStr)) 
				return;

			if(next!=null && !Util.isValidFASTAFormat(next)) 
				return;

			countGenomeStrings.increment(1);

			//In questo caso un file contiene una sola sequenza e questi record sono relativi tutti alla stessa sequenza.
			if(idSequence==null)
				idSequence = new Text(idSeq.toString()); 
			length+=genomeStr.length();
			//Prima: mos.write(Constant.TEXT, idSeq, genomeStr.length(), HDFS_HOME_DIR+Constant.HDFS_ID_SEQS_FILE);


			if(Constant.DEBUG_MODE){
				String filename= ((FileSplit)context.getInputSplit()).getPath().getName();
				System.out.println("Filename: "+filename+": <"+idSeq+", ("+genomeStr+","+next+")>");
			}
			
			

			if(next!=null){
				size = genomeStr.length();
				genomeStr+=next;
			}	
			
			for(String pattern : patterns){

				if(Util.isValidSpacedWordPattern(pattern)){
					

					k = pattern.length();

					if(next==null)// Ultima linea del file di input.
						size = genomeStr.length() - k + 1;

					/* cycle over the length of String till k-mers of length, k, can still be made */
					for(int i=0; i<size; i++){
						/* output each k-mer of length k, from i to i+k in String*/
						
						//Devo gestire il problema relativo all'eventuale lunghezza minore dell'ultima riga.
						if(genomeStr.substring(i).length()>=k){
							kmer = genomeStr.substring(i, i+k);
							incrementValue(kmer, idSeq.toString(), pattern);
	
						}
					}
					
					
				}
				
				
			}


		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
	}

	private void incrementValue(String kmer, String idSeq, String pattern) {
		
		if(hasCount)
			incrementCountValue(kmer, idSeq,pattern);

		if(hasCO){
			if(Util.isCompatibile(pattern))
				incrementCOValue(kmer, idSeq,pattern); 
			}
	}


	private void incrementCountValue(String kmer, String idSeq, String pattern) {

		KmerSeq mapKey;

		if(pattern.contains("0")){
			String spacedWord = Util.extractSpacedWord(kmer, pattern); //kmer deve essere una spaced-word (inexact match).
			mapKey = new KmerSeq(spacedWord, idSeq);
		}
		else //kmer contiene tutti 1 (ossia e' un exact match).
			mapKey = new KmerSeq(kmer, idSeq);

		Integer mapValue = mapCount.get(mapKey); 

		if(mapValue==null)
			mapCount.put(mapKey, new Integer(1));
		else
			mapCount.put(mapKey, mapValue + 1);
		

	}



	private void incrementCOValue(String kmer, String idSeq, String pattern) {

		KmerSeq context;
		String[] CO = Util.extractCurrentCO(kmer, pattern);

		context = new KmerSeq(CO[0], idSeq);

		Text val = mapObject.get(context); 

		if(val!=null){
			if(!val.toString().equals(Constant.MARKED))
				if(!CO[1].equalsIgnoreCase(val.toString()))
					mapObject.put(context, new Text(Constant.MARKED));
		}
		else{
			mapObject.put(context, new Text(CO[1]));
		}

	}



	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		super.cleanup(context);
		cleanupKmerCount(context);
		cleanupKmerCO(context);


		if(idSequence!=null && length>0)
			mos.write(Constant.TEXT, idSequence, length, HDFS_HOME_DIR+Constant.HDFS_ID_SEQS_FILE);

		try {
			mos.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private void cleanupKmerCount(Context context) throws IOException, InterruptedException{
		Iterator<Entry<KmerSeq, Integer>> it = mapCount.entrySet().iterator();
		Entry<KmerSeq, Integer> kv;
		
		while(it.hasNext()){
			kv = it.next();
			//System.out.println(kv);

			kword.set(kv.getKey().getKmer());
			id_Seq.set(kv.getKey().getIdSeq());
			intero.set(kv.getValue());

			kcw.setIdSeq(id_Seq);
			kcw.setCount(intero);


			if(Constant.DEBUG_MODE)
				System.out.println(kword+" "+kcw);

			myw.set(kcw);
			context.write(kword,myw);
			
			//mapCount.remove(kv.getKey());
		}

		mapCount.clear();
	}

	private void cleanupKmerCO(Context context) throws IOException, InterruptedException{
		Iterator<Entry<KmerSeq, Text>> it = mapObject.entrySet().iterator();
		Entry<KmerSeq, Text> kv;

		while(it.hasNext()){
			kv = it.next();
			//System.out.println(kv);

			kword.set(kv.getKey().getKmer());
			id_Seq.set(kv.getKey().getIdSeq());
			object.set(kv.getValue().toString());

			kco.setIdSeq(id_Seq);
			kco.setObject(object);


			if(Constant.DEBUG_MODE)
				System.out.println(kword+" "+kco);

			myw.set(kco);
			context.write(kword, myw);
			
			//mapObject.remove(kv.getKey());
		}

		mapObject.clear();
	}

}
