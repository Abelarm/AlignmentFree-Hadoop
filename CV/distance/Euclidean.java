package CV.distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 30 2015
 */
public class Euclidean implements DistanceMeasure {
	

	public Euclidean() {
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
		return Math.sqrt(dist);
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
		return "Euclidean";
	}

	public boolean isCompatibile(String pattern){
		
			return true;
		
	}
}
