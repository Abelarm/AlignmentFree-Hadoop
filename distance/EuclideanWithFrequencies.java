package distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.2
 * 
 * Date: January, 30 2015
 */
public class EuclideanWithFrequencies extends Euclidean {
	

	public EuclideanWithFrequencies() {
		super();
	}
	
	
	@Override
	public double computePartialDistance(Parameters param) {
		
		double[] v = DistanceMeasure.getNormalizedValues(param.getC1(), param.getLength1(), param.getC2(), param.getLength2(), param.getK());

		return Math.pow((v[0]-v[1]), 2);

	}
	
	@Override
	public String getName() {
		return "EuclideanWithFrequencies";
	}
	
}
