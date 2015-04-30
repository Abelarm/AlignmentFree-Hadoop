package CV.distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 30 2015
 */
public class ChebychevWithFrequencies extends Chebychev {
	

	public ChebychevWithFrequencies() {
		super();
	}
	
	/*
	@Override
	public double computePartialDistance(int c1, int length1, int c2, int length2, int k) {
				
		double[] v = DistanceMeasure.getNormalizedValues(c1, length1, c2, length2, k);
		
		return Math.abs(v[0]-v[1]);
	}*/
	
	@Override
	public double computePartialDistance(Parameters param){
		
		double[] v = DistanceMeasure.getNormalizedValues(param.getC1(), param.getLength1(), param.getC2(), param.getLength2(), param.getK());
		
		return Math.abs(v[0]-v[1]);
	}


	@Override
	public String getName() {
		return "ChebychevWithFrequencies";
	}

}
