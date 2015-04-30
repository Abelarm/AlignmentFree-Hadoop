package hadoop.piarwisesimilarity;

import hadoop.ArrayKmerCountWritable;
import hadoop.KmerCOWritable;
import hadoop.DistanceDoubleWritable;
import hadoop.HadoopUtil;
import hadoop.IdSeqsWritable;
import hadoop.KmerCountWritable;
import hadoop.KmerGenericWritable;
import hadoop.SequenceInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.*;

import distance.ContextObject;
import distance.DistanceMeasure;
import distance.Parameters;
import distance.d2.EstimatedProb;
import distance.d2.FixedProb;
import distance.d2.UniformProb;

import utility.Constant;
import utility.Util;



/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 2.8
 * 
 * Date: February, 15 2015
 */

public class PairwiseSimilarityMapper extends Mapper<Text, KmerGenericWritable, IdSeqsWritable, DistanceDoubleWritable> {	

	private final IdSeqsWritable outputKey = new IdSeqsWritable();
	//private final static KmerCountsWritable outputValue = new KmerCountsWritable();
	private final Parameters param = new Parameters();
	private Map<String,List<Entry<String,Integer>>> mapl1 = new HashMap<String,List<Entry<String,Integer>>>();
	//private final DoubleWritable distWritable = new DoubleWritable();
	//private final IntWritable countWritable = new IntWritable();

	private final DistanceDoubleWritable outputValueDouble = new DistanceDoubleWritable();

	private List<SequenceInfo> idAllSeqs;
	private Counter countErrors;
	private Counter countFunctions;
	private List<DistanceMeasure> distances; 
	private final Text patternName = new Text("");
	private final Text distanceName = new Text("");
	private int k;
	private String probPath = "";
	private HashMap<String, Map<String, Double>>  probabilities = null;

	@Override
	protected void setup(

			Mapper<Text,KmerGenericWritable, IdSeqsWritable, DistanceDoubleWritable>.Context context)	throws IOException, InterruptedException {
		super.setup(context);


		distances = HadoopUtil.readDistances(context.getConfiguration()); //Legge le distanze dalla configurazione.

		FileSystem fs = FileSystem.get(context.getConfiguration());

		countErrors = context.getCounter("PairwiseSimilarity Mapper","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("PairwiseSimilarity Mapper","number of functions");
		countFunctions.setValue(0);

		//Path[] localPaths = context.getLocalCacheFiles();
		URI[] localPaths = context.getCacheFiles();

		boolean fixed=false;
		boolean estimated=false;
		for(DistanceMeasure c : distances){
			if(fixed && estimated)
				break;
			if(c instanceof FixedProb && !fixed){
				fixed=true;
			}else{
				if(c instanceof EstimatedProb && !estimated){
					estimated=true;
				}
			}
		}

		if(localPaths.length>0){
			idAllSeqs = readHdfsFile(fs, new Path(localPaths[0]));

			if(localPaths.length == 2){
				if(fixed){
					probPath = localPaths[1].toString();
					probabilities = readProbabilities(Constant.PROBABILITIES_PATH);
				}
				else
					if(estimated)
						readSeqFile(new Path(localPaths[1]));
			}else{
				if(localPaths.length==3){
					readSeqFile(new Path(localPaths[1]));
					probPath = localPaths[2].toString();	
					probabilities = readProbabilities(Constant.PROBABILITIES_PATH);
				}
			}	

			if(Constant.DEBUG_MODE)
				System.out.println(idAllSeqs);
		}

	}


	public static List<SequenceInfo> readHdfsFile(FileSystem fs, Path pt){

		Set<SequenceInfo> idSeqs = new HashSet<SequenceInfo>();
		SequenceInfo info;

		try{
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			String s[];
			line=br.readLine();
			while (line != null){
				s = line.split("[\t]");

				info = new SequenceInfo();

				info.setId(new Text(s[0]));

				if(s.length>1)
					info.setLength(new Integer(s[1]));

				idSeqs.add(info);

				line=br.readLine();

			}
		}catch(Exception e){
			e.printStackTrace();
		}

		List<SequenceInfo> res = new ArrayList<SequenceInfo>(idSeqs);
		//Collections.shuffle(res);

		return res;
	}

	@Override
	public void map(Text kword, KmerGenericWritable value, Context context) throws IOException, InterruptedException {

		countFunctions.increment(1);
		context.progress();
		try{
			Writable w = value.get();
			if(w instanceof ArrayWritable)
				computeSimilarity(kword, ((ArrayWritable)w).get(), context);
		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
		context.progress();


	}

	private void computeSimilarity(Text kword, Writable[] writables, Context context) throws IOException, InterruptedException{



		Map<Text,Integer> idSeqsCounts = new HashMap<Text,Integer>();
		Map<Text,Text> idSeqsCO = new HashMap<Text,Text>();
		KmerCountWritable kcw = null;
		KmerCOWritable kco = null;

		patternName.set(Util.extractPattern(kword.toString()));

		k = kword.toString().length();

		if(Constant.DEBUG_MODE)
			System.out.println(kword);

		for(Writable w : writables){

			if(w instanceof KmerCountWritable){
				kcw = (KmerCountWritable) w;
				idSeqsCounts.put(kcw.getIdSeq(), kcw.getCount().get());
			}

			else if(w instanceof KmerCOWritable){
				kco = (KmerCOWritable) w;
				idSeqsCO.put(kco.getIdSeq(), kco.getObject());

			}
		}

		for(int i=0; i<idAllSeqs.size(); i++) 
			for(int j=i+1; j<idAllSeqs.size(); j++){

				for(DistanceMeasure distance : distances){

					distanceName.set(distance.getClass().getName());

					//Per ogni coppia di sequenze salvo in un oggetto il kword e i rispettivi contatori (0 se uno non esite).


					if(!distance.isCompatibile(patternName.toString()))
						continue;

					if (distance instanceof ContextObject){

						//if(!kword.toString().contains(Constant.JOLLY_CHARACTER+"")){
						computeCO(kword.toString(), i, j, idSeqsCO, context, distance);
						//}
					}
					else{

						if(distance.isSymmetricMeasure())
							computeSymmetric(kword.toString(), i, j, idSeqsCounts, context, distance);
						else
							computeAsymmetric(kword.toString(), i, j, idSeqsCounts, context, distance);
					}
				}
			}
	}

	private void computeCO(String contesto, int i, int j, Map<Text, Text> idSeqsCO, Context context, DistanceMeasure distance) throws IOException, InterruptedException {



		Text idSeq1, idSeq2;

		idSeq1 = idAllSeqs.get(i).getId();
		idSeq2 = idAllSeqs.get(j).getId();

		Text object1 = idSeqsCO.get(idSeq1);
		Text object2 = idSeqsCO.get(idSeq2);


		if(object1!=null && object2!=null) {

			if(!object1.toString().equals(Constant.MARKED) && !object2.toString().equals(Constant.MARKED)){

				if(Constant.DEBUG_MODE){
					System.out.println(idSeq1.toString()+"-"+idSeq2.toString()+"- contesto: "+contesto+"\toggetto1: "+object1+"\toggetto2: "+object2);
				}

				param.setContesto1(contesto);
				param.setContesto2(contesto);
				param.setOggetto1(object1.toString());
				param.setOggetto2(object2.toString());
				param.setK(k);

				double dist = distance.computePartialDistance(param); 

				outputKey.setIdSeq1(idSeq1);
				outputKey.setIdSeq2(idSeq2);
				outputKey.setPattern(patternName);
				outputKey.setDistanceClass(distanceName);
				outputValueDouble.getDist().set(dist);
				outputValueDouble.getCount().set(1);

				if(Constant.DEBUG_MODE){
					System.out.println(outputKey+" "+outputValueDouble);
				}
				context.write(outputKey, outputValueDouble);
			}
		}
	}



	private void computeSymmetric(String kword, int i, int j, Map<Text, Integer> idSeqsCounts, Context context, DistanceMeasure distance) throws IOException, InterruptedException {

		Text idSeq1, idSeq2;
		Integer c1, c2, l1, l2, val;
		boolean flag1=false, flag2=false;


		if(idAllSeqs.get(i).getId().toString().compareTo(idAllSeqs.get(j).getId().toString())<0){		
			idSeq1 = idAllSeqs.get(i).getId();
			idSeq2 = idAllSeqs.get(j).getId();
			l1 = idAllSeqs.get(i).getLength();
			l2 = idAllSeqs.get(j).getLength();


			val = idSeqsCounts.get(idAllSeqs.get(i).getId());
			if(val==null){
				c1=0;
				flag1=true;
			}
			else 
				c1=val;

			val = idSeqsCounts.get(idAllSeqs.get(j).getId());
			if(val==null){
				flag2=true;
				c2=0;
			}
			else 
				c2=val;
		}
		else{
			idSeq1 = idAllSeqs.get(j).getId();
			idSeq2 = idAllSeqs.get(i).getId();
			l1 = idAllSeqs.get(j).getLength();
			l2 = idAllSeqs.get(i).getLength();



			val = idSeqsCounts.get(idAllSeqs.get(j).getId());
			if(val==null){
				c1=0;
				flag1=true;
			}
			else 
				c1=val;

			val = idSeqsCounts.get(idAllSeqs.get(i).getId());
			if(val==null){
				c2=0;
				flag2=true;
			}
			else 
				c2=val;
		}


		if(flag1 && flag2) //Se tutti e due i contatori sono a zero.
			return;

		if(distance.hasInternalProduct()) //C'e' il prodotto nella computazione della "distanza locale".
			if(flag1 || flag2) //C'e' un contatore a 0.
				return;


		Parameters param = new Parameters();
		param.setC1(c1);
		param.setC2(c2);
		param.setLength1(l1);
		param.setLength2(l2);
		param.setK(k);
		param.setKmer(kword);


		if(distance instanceof EstimatedProb){
			List<Entry<String,Integer>> li1 = mapl1.get(idSeq1.toString());
			List<Entry<String,Integer>> li2 = mapl1.get(idSeq2.toString());
			Map<String,Integer> m1 = new HashMap<String,Integer>();
			Map<String,Integer> m2 = new HashMap<String,Integer>();
			Iterator<Entry<String,Integer>> it;			
			Entry<String,Integer> e = null;

			it = li1.iterator();

			while(it.hasNext()){
				e=it.next();

				m1.put(e.getKey(), e.getValue());

			}

			it=li2.iterator();
			while(it.hasNext()){
				e=it.next();

				m2.put(e.getKey(), e.getValue());

			}
			param.setMapS1(m1);
			param.setMapS2(m2);


		}


		if(distance instanceof UniformProb){
			if(Constant.IS_PROTEIN)
				param.setNumLettere(Constant.PROTEIN_ALPHABET.length());
			else
				param.setNumLettere(Constant.DNA_ALPHABET.length());
		}

		if(distance instanceof FixedProb){
			if(probabilities.containsKey("ALL")){
				param.setProbMap1(probabilities.get("ALL"));
				param.setProbMap2(probabilities.get("ALL"));
			}else{
				if(probabilities.containsKey("all")){
					param.setProbMap1(probabilities.get("all"));
					param.setProbMap2(probabilities.get("all"));
				}else{
					param.setProbMap1(probabilities.get(idSeq1.toString()));
					param.setProbMap2(probabilities.get(idSeq2.toString()));
				}
			}
		}


		double dist = distance.computePartialDistance(param); 

		//double dist = distance.computePartialDistance(c1, l1, c2, l2, k);

		//		if(Constant.DEBUG_MODE)
		//			System.out.println(idSeq1+"="+c1+" "+idSeq2+"="+c2+" Dist="+dist);


		//outputKey = new IdSeqsWritable(idSeq1, idSeq2);
		//outputValue = new KmerCountsWritable(kword, c1, c2); 

		outputKey.setIdSeq1(idSeq1);
		outputKey.setIdSeq2(idSeq2);
		outputKey.setPattern(patternName);
		outputKey.setDistanceClass(distanceName);
		outputValueDouble.getDist().set(dist);
		outputValueDouble.getCount().set(1);

		if(Constant.DEBUG_MODE)
			System.out.println(outputKey+" "+outputValueDouble);

		context.write(outputKey, outputValueDouble);

	}




	private void computeAsymmetric(String kword, int i, int j, Map<Text, Integer> idSeqsCounts, Context context, DistanceMeasure distance) throws IOException, InterruptedException {

		Text idSeq1, idSeq2;
		Integer c1, c2, l1, l2, val;
		boolean flag1=false, flag2=false;

		idSeq1 = idAllSeqs.get(i).getId();
		idSeq2 = idAllSeqs.get(j).getId();
		l1 = idAllSeqs.get(i).getLength();
		l2 = idAllSeqs.get(j).getLength();

		val = idSeqsCounts.get(idAllSeqs.get(i).getId());
		if(val==null){
			c1=0;
			flag1=true;
		}
		else 
			c1=val;

		val = idSeqsCounts.get(idAllSeqs.get(j).getId());
		if(val==null){
			flag2=true;
			c2=0;
		}
		else 
			c2=val;


		if(flag1 && flag2) //Se tutti e due i contatori sono a zero.
			return;

		if(distance.hasInternalProduct()) //C'e' il prodotto nella computazione della "distanza locale".
			if(flag1 || flag2) //C'e' un contatore a 0.
				return;

		//Parte 1
		param.setC1(c1);
		param.setC2(c2);
		param.setLength1(l1);
		param.setLength2(l2);
		param.setK(k);
		param.setKmer(kword);
		double dist = distance.computePartialDistance(param); 
		//double dist = distance.computePartialDistance(c1, l1, c2, l2, k);

		//		if(Constant.DEBUG_MODE)
		//			System.out.println(idSeq1+"="+c1+" "+idSeq2+"="+c2+" Dist="+dist);

		outputKey.setIdSeq1(idSeq1);
		outputKey.setIdSeq2(idSeq2);
		outputKey.setPattern(patternName);
		outputKey.setDistanceClass(distanceName);
		outputValueDouble.getDist().set(dist);
		outputValueDouble.getCount().set(1);

		if(Constant.DEBUG_MODE)
			System.out.println(outputKey+" "+outputValueDouble);

		context.write(outputKey, outputValueDouble);

		//Parte 2
		param.setC1(c2);
		param.setC2(c1);
		param.setLength1(l2);
		param.setLength2(l1);
		param.setK(k);
		param.setKmer(kword);
		dist = distance.computePartialDistance(param); 
		//dist = distance.computePartialDistance(c2, l2, c1, l1, k);

		//		if(Constant.DEBUG_MODE)
		System.out.println(idSeq2+"="+c2+" "+idSeq1+"="+c1+" Dist="+dist);

		outputKey.setIdSeq1(idSeq2);
		outputKey.setIdSeq2(idSeq1);
		outputKey.setPattern(patternName);
		outputKey.setDistanceClass(distanceName);
		outputValueDouble.getDist().set(dist);
		outputValueDouble.getCount().set(1);

		if(Constant.DEBUG_MODE)
			System.out.println(outputKey+" "+outputValueDouble);

		context.write(outputKey, outputValueDouble);
	}

	private void readSeqFile(Path path) throws IOException {
		Configuration conf = new Configuration();


		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));

		Text key = new Text();
		KmerGenericWritable val = new KmerGenericWritable();



		while (reader.next(key, val)) {

			Writable w = val.get();
			ArrayKmerCountWritable v = null;
			KmerCountWritable s = null;
			if(w instanceof ArrayKmerCountWritable){
				v = (ArrayKmerCountWritable) w;
				for(int i=0; i<v.get().length; i++){
					if(v.get()[i] instanceof KmerCountWritable){

						s = (KmerCountWritable) v.get()[i];
						List<Entry<String,Integer>> valore =new ArrayList<Entry<String,Integer>>();

						Entry<String, Integer> entry =  new AbstractMap.SimpleEntry<String, Integer>(key.toString(), s.getCount().get());

						if(mapl1.get(s.getIdSeq().toString())==null){
							valore.add(entry);
							mapl1.put(s.getIdSeq().toString(), valore);

						}
						else{
							mapl1.get(s.getIdSeq().toString()).add(entry);
							mapl1.get(key);
						}

					}
				}
			}

		}

		if(reader!=null)
			reader.close();
	}



	private static HashMap<String, Map<String, Double>> readProbabilities(String probFile) { 

		HashMap<String,Map<String, Double>> listaProb = new HashMap<String, Map<String, Double>>();
		HashMap<String, Double> prob=null;

		try {
			Scanner in = new Scanner(new File(probFile));

			String currLine;
			String currID="";
			while(in.hasNextLine()){
				currLine = in.nextLine();
				if(currLine.contains(">")){
					if(prob!=null)
						listaProb.put(currID, prob); //metto la mappa precedente
					currID=currLine.substring(1);
					prob = new HashMap<String, Double>();
				}
				else{
					String letter=currLine.substring(0, 1);
					double probability = Double.parseDouble(currLine.substring(2));
					prob.put(letter, probability);
				}
			}
			listaProb.put(currID, prob); //inserisco l'ultima mappa
			in.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getLocalizedMessage());
		}

		return listaProb;
	}
}