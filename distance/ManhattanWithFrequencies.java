package distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 30 2015
 */
public class ManhattanWithFrequencies extends Manhattan {
	

	public ManhattanWithFrequencies() {
		super();
	}

	@Override
	public double computePartialDistance(Parameters param) {
		
		double[] v = DistanceMeasure.getNormalizedValues(param.getC1(), param.getLength1(), param.getC2(), param.getLength2(), param.getK());
		
		return Math.abs(v[0]-v[1]);
	}
	
	
	@Override
	public String getName() {
		return "ManhattanWithFrequencies";
	}
	
}
