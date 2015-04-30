package hadoop.piarwisesimilarity;

import hadoop.DistanceDoubleWritable;
import hadoop.HadoopUtil;
import hadoop.IdSeqsWritable;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import distance.CoPhylogDistance;
import distance.DistanceMeasure;
import utility.Constant;

/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 2.5
 * 
 * Date: February, 15 2015
 */
public class PairwiseSimilarityReducer extends Reducer<IdSeqsWritable, DistanceDoubleWritable, IdSeqsWritable, DoubleWritable> {

	//private final DistanceDoubleWritable dist = new DistanceDoubleWritable(); 
	private final DoubleWritable finalResult = new DoubleWritable();
	private Counter countErrors;
	private Counter countFunctions;
	private MultipleOutputs<Text, Writable> mos;


	@Override
	protected void setup(
			Reducer<IdSeqsWritable, DistanceDoubleWritable, IdSeqsWritable, DoubleWritable>.Context context)
					throws IOException, InterruptedException {
		super.setup(context);
		
		countErrors = context.getCounter("PairwiseSimilarity Reducer","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("PairwiseSimilarity Reducer","number of functions");
		countFunctions.setValue(0);
	}

	@Override
	public void reduce(IdSeqsWritable key, Iterable<DistanceDoubleWritable> values, Context context) throws IOException, InterruptedException {


		countFunctions.increment(1);

		try{
			int numEl=0;
			DistanceMeasure distance = HadoopUtil.getDistanceMeasure(context.getConfiguration(), key.getDistanceClass().toString());

			double result = distance.initDistance() ; //Puo' essere 0 per la somma o 1 per il prodotto a secondo del tipo di distanza usata.

			for (DistanceDoubleWritable value : values){
				numEl+=value.getCount().get();
				result = distance.distanceOperator(result,value.getDist().get());
			}
			
			result = distance.finalizeDistance(result,numEl);
			finalResult.set(result);
			//dist.getDist().set(result);
			//dist.getCount().set(numEl);


			if(Constant.DEBUG_MODE){
				System.out.println("Contesti totali: "+numEl+"\tOggetti totali: "+finalResult.get()*numEl);
				System.out.println("Distanza tra: "+key.getIdSeq1().toString()+"-"+key.getIdSeq2().toString()+"\t"+finalResult.get());

			}

			context.write(key, finalResult); 




			if(Constant.DEBUG_MODE)
				System.out.println("Pattern "+key.getPattern()+" d_"+distance+"("+key.getIdSeq1()+","+key.getIdSeq2()+")=" +finalResult.get());

		}
		catch(Exception e){
			e.printStackTrace();
			countErrors.increment(1);
		}
	}


	//	@Override
	//	public void reduce(IdSeqsWritable key, Iterable<KmerCountsWritable> values, Context context) throws IOException, InterruptedException {
	//
	//		System.out.print(key+":");
	//
	//		double sum = 0.0, val1, val2;
	//		int count1=0, count2=0;
	//		ArrayList<Integer> v1 = new ArrayList<Integer>();
	//		ArrayList<Integer> v2 = new ArrayList<Integer>();
	//
	//		for (KmerCountsWritable value : values){
	//			System.out.print(value+" ");distance
	//			count1+=value.getC1().get();
	//			count2+=value.getC2().get();
	//			v1.add(value.getC1().get());
	//			v2.add(value.getC2().get());
	//		}
	//		System.out.println();
	//
	//		//System.out.println("Den1: "+den1);
	//		//System.out.println("Den2: "+den2);
	//
	//		for (int i=0; i<v1.size(); i++) {
	//			val1 = (double)v1.get(i)/count1;
	//			val2 = (double)v2.get(i)/count2;
	//			System.out.println(val1+" "+val2);
	//			sum+=Math.pow((val1-val2), 2);
	//		}
	//
	//		//System.out.println("SOMMA="+sum);
	//
	//		dist.set(sum);
	//		context.write(key, dist);
	//
	//		//System.out.println(key);
	//	}

}