package CV.hadoop.similarity;

import hadoop.IdSeqsWritable;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import CV.distance.CoPhylogDistance;
import CV.distance.DistanceMeasure;
import CV.distance.DistanceMeasureDouble;
import CV.distance.PercentageOfDisagreement;
import CV.hadoop.KmerPartialCVWritable;
import utility.Constant;

/**
 * Reducer: Computes the distance of genomes according to the Composition Vector method
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia
 * 
 * @version 3.0
 * 
 * Date: February, 16 2015
 */
public class CVSimilarityReducer extends Reducer<IdSeqsWritable, KmerPartialCVWritable, IdSeqsWritable, DoubleWritable>{
	private final DoubleWritable finalResult = new DoubleWritable();
	//private final HashMap<Integer,ArrayList<Double>> output= new HashMap<Integer,ArrayList<Double>>();
	private Counter countErrors;
	private Counter countFunctions;
	//private MultipleOutputs<Text, Writable> mos;
	
	@Override
	protected void setup(
			Reducer<IdSeqsWritable, KmerPartialCVWritable, IdSeqsWritable, DoubleWritable>.Context context)
					throws IOException, InterruptedException {
		super.setup(context);

		countErrors = context.getCounter("CV_A Reducer","errors");
		countErrors.setValue(0);

		countFunctions = context.getCounter("CV_A Reducer","number of functions");
		countFunctions.setValue(0);
	}

	@Override
	public void reduce(IdSeqsWritable key, Iterable<KmerPartialCVWritable> values, Context context) throws IOException, InterruptedException {

		countFunctions.increment(1);

		try{
			double result = 0 ; 
			ArrayList<Double> Aresult = new ArrayList<Double>();
			Aresult.add(0,0.0);
			Aresult.add(1,0.0);
			Aresult.add(2,0.0);
			
			DistanceMeasure distance = getDistanceMeasure(context.getConfiguration(), key.getDistanceClass().toString());
			
			if(distance instanceof PercentageOfDisagreement || distance instanceof CoPhylogDistance || distance.getName().contains("D2S"))
				return;
			
			result = distance.initDistance();
			
			for (KmerPartialCVWritable value : values){
				if(distance instanceof DistanceMeasureDouble){
					ArrayList<Double> partial = new ArrayList<Double>();
					partial.add(value.getTop().get());
					partial.add(value.getDown1().get());
					partial.add(value.getDown2().get());
					Aresult = ((DistanceMeasureDouble) distance).distanceOperatorDouble(partial, Aresult);
				}else{
					result = distance.distanceOperator(value.getTop().get(), result);
				}
			}
			
			if(distance instanceof DistanceMeasureDouble){
				result = ((DistanceMeasureDouble)distance).finalizeDistanceDouble(Aresult, 0);
				finalResult.set(result);
			}else{
				result = distance.finalizeDistance(result, 0);
				finalResult.set(result);
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
