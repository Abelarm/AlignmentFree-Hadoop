package CV.hadoop.similarity;

import hadoop.IdSeqsWritable;
import hadoop.SequenceInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import CV.distance.CoPhylogDistance;
import CV.distance.DistanceMeasure;
import CV.distance.DistanceMeasureDouble;
import CV.distance.Parameters;
import CV.distance.PercentageOfDisagreement;
import CV.hadoop.ArrayKmer4CountsWritable;
import CV.hadoop.Kmer4CountsWritable;
import CV.hadoop.KmerPartialCVWritable;
import utility.Constant;
import utility.Util;


/**
 * Mapper: Computes the distance of genomes according to the Composition Vector method
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 3.0
 * 
 * Date: February, 16 2015
 */
public class CVSimilarityMapper extends Mapper<Text, ArrayKmer4CountsWritable, IdSeqsWritable, KmerPartialCVWritable> {	
	
	private final IdSeqsWritable outputKey = new IdSeqsWritable();
	private final Parameters param = new Parameters();
	private final KmerPartialCVWritable outputValue = new KmerPartialCVWritable();
	private List<SequenceInfo> idAllSeqs;
	private Counter countErrors;
	private Counter countFunctions;
	private List<DistanceMeasure> distances; 
	private final Text distanceName = new Text("");
	private final Text patternName = new Text("");
	private int k;

	@Override
	protected void setup(
			Mapper<Text, ArrayKmer4CountsWritable, IdSeqsWritable, KmerPartialCVWritable>.Context context)
					throws IOException, InterruptedException {
		super.setup(context);
		
				
		distances = readDistances(context.getConfiguration()); //Legge le distanze dalla configurazione.
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		countErrors = context.getCounter("CV_A Mapper","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("CV_A Mapper","number of functions");
		countFunctions.setValue(0);

		//Path[] localPaths = context.getLocalCacheFiles();
		URI[] localPaths = context.getCacheFiles();

		if(localPaths.length>0){
			idAllSeqs = readHdfsFile(fs, new Path(localPaths[0]));

			if(Constant.DEBUG_MODE)
				System.out.println(idAllSeqs);
		}

	}


	private static List<SequenceInfo> readHdfsFile(FileSystem fs, Path pt){

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

		//System.out.println(idSeqs);

		List<SequenceInfo> res = new ArrayList<SequenceInfo>(idSeqs);
		//Collections.shuffle(res);

		return res;
	}

	@Override
	public void map(Text kword, ArrayKmer4CountsWritable value, Context context) throws IOException, InterruptedException {
		
		countFunctions.increment(1);
		context.progress();
		try{

			computeSimilarity(kword, value.get(), context);

		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
		context.progress();
	}

	private void computeSimilarity(Text kword, Writable[] writables, Context context) throws IOException, InterruptedException{

		Map<Text,Double> idSeqsA = new HashMap<Text,Double>();
		Kmer4CountsWritable kcw = null;
		
		patternName.set(Util.extractPattern(kword.toString()));
		k = kword.toString().length();

		/*if(Constant.DEBUG_MODE)
			System.out.println(kword);*/
		
		String id="";
		SequenceInfo currentSeq=null;
		int seqLength=0;
		double p, p1, p1b, p2, p0, a;
		
		for(Writable w : writables){

			if(w instanceof Kmer4CountsWritable){
				kcw = (Kmer4CountsWritable) w;
				id=kcw.getIdSeq().toString();
				for(int i=0;i<idAllSeqs.size();i++){
					currentSeq=idAllSeqs.get(i);
					if(id.equals(currentSeq.getId().toString())){
						seqLength=currentSeq.getLength();
						break;
					}
				}
				p=(kcw.getCount0().get())/(seqLength-k+1);
				p1=(kcw.getCount1().get())/(seqLength-(k-1)+1);
				p1b=(kcw.getCount1b().get())/(seqLength-(k-1)+1);
				p2=(kcw.getCount2().get())/(seqLength-(k-2)+1);
				if(p2==0)
					p0=0;
				else
					p0=(p1*p1b)/p2;
				a=(p-p0)/p0;
				idSeqsA.put(kcw.getIdSeq(), a);
				//System.out.print(kcw+" ");
			}
		}

		//System.out.println("\n"+idSeqs);
		//System.out.println("SET ALL: "+idAllSeqs);
		double a1, a2;
		int l1, l2;
		for(int i=0; i<idAllSeqs.size(); i++){
			for(int j=i+1; j<idAllSeqs.size(); j++){

				for(DistanceMeasure distance : distances){
					if(distance instanceof PercentageOfDisagreement || distance instanceof CoPhylogDistance || distance.getName().contains("D2S"))
						continue;
					distanceName.set(distance.getClass().getName());
					outputKey.setIdSeq1(idAllSeqs.get(i).getId());
					outputKey.setIdSeq2(idAllSeqs.get(j).getId());
					outputKey.setPattern(patternName);
					outputKey.setDistanceClass(distanceName);
					
					Text id_I = idAllSeqs.get(i).getId();
					Text id_J = idAllSeqs.get(j).getId();
					if(idSeqsA.containsKey(id_I))
						a1 = idSeqsA.get(id_I);
					else
						a1=0;
					if(idSeqsA.containsKey(id_J))
						a2 = idSeqsA.get(id_J);
					else
						a2=0;
					
					l1 = idAllSeqs.get(i).getLength();
					l2 = idAllSeqs.get(j).getLength();
					
					param.setC1(a1);
					param.setC2(a2);
					param.setLength1(l1);
					param.setLength2(l2);
					param.setK(k);
					param.setKmer(kword.toString());
					
					outputValue.setKmer(kword);
					if(distance instanceof DistanceMeasureDouble){
						ArrayList<Double> partial = ((DistanceMeasureDouble)distance).computePartialDistanceDouble(param);
						outputValue.setTop(new DoubleWritable(partial.get(0)));
						outputValue.setDown1(new DoubleWritable(partial.get(1)));
						outputValue.setDown2(new DoubleWritable(partial.get(2)));
					}else{
						double part = distance.computePartialDistance(param);
						outputValue.setTop(new DoubleWritable(part));
						outputValue.setDown1(new DoubleWritable(0.0));
						outputValue.setDown2(new DoubleWritable(0.0));
					}
					
					
					

					if(Constant.DEBUG_MODE)
					System.out.println(outputKey+" "+outputValue);

					context.write(outputKey, outputValue);
				}
			}
		}
	}
	
	public static List<DistanceMeasure> readDistances(Configuration conf) {//Usato in Hadoop

		int n = Integer.parseInt(conf.get("NumDistances"));

		List<DistanceMeasure> dists = new ArrayList<DistanceMeasure>(); 

		for(int i=1; i<=n; i++){

			String className = conf.getClass("Distance"+i, DistanceMeasure.class).getName();

			DistanceMeasure dm = getDistanceMeasure(conf, className);

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
}
