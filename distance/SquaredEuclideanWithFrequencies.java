package distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.3
 * 
 * Date: January, 30 2015
 */
public class SquaredEuclideanWithFrequencies extends SquaredEuclidean {
	

	public SquaredEuclideanWithFrequencies() {
		super();
	}
	

	/*@Override
	public double computePartialDistance(int c1, int length1, int c2, int length2, int k) {
		
		double[] v = DistanceMeasure.getNormalizedValues(c1, length1, c2, length2, k);

		return Math.pow((v[0]-v[1]), 2);
	}*/
	
	@Override
	public double computePartialDistance(Parameters param) {
		
		double[] v = DistanceMeasure.getNormalizedValues(param.getC1(), param.getLength1(), param.getC2(), param.getLength2(), param.getK());

		return Math.pow((v[0]-v[1]), 2);
	}
	
//	@Override
//	public double computePartialDistance(int c1, int length1, int c2, int length2, int k) {
//		
//		double v1=0;
//		double v2=0;
//
//		int den1 = length1 - k + 1;
//		int den2 = length2 - k + 1;
//
//		//System.out.println(c1+ " " +length1+ " " + c2 + " " + length2);
//
//		if(c1!=0 && length1!=0 && den1>0)
//			v1 = (double) c1/den1;
//
//		if(c2!=0 && length2!=0 && den2>0)
//			v2 = (double) c2/den2;
//
//		return Math.pow((v1-v2), 2);
//	}

	@Override
	public String getName() {
		return "SquaredEuclideanWithFrequencies";
	}
	
}
