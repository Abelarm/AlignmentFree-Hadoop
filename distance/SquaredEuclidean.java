package distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.3
 * 
 * Date: January, 30 2015
 */
public class SquaredEuclidean implements DistanceMeasure {
	

	public SquaredEuclidean() {
		super();
	}
	
	@Override
	public double computePartialDistance(Parameters param) {

		return Math.pow((param.getC1()-param.getC2()), 2);
	}

	@Override
	public double distanceOperator(double partialResult, double addResult) {
		return partialResult + addResult;
	}

	@Override
	public double initDistance() {
		return 0.0;
	}

	@Override
	public boolean hasInternalProduct() {
		return false;
	}

	@Override
	public double finalizeDistance(double dist, int numEl) {
		return dist;
	}

	@Override
	public boolean isSymmetricMeasure() {
		return true;
	}
	
	@Override
	public String toString() {
		return getName();
	}


	@Override
	public String getName() {
		return "SquaredEuclidean";
	}
	
	public boolean isCompatibile(String pattern){
		
			return true;
		
	}
	
}
