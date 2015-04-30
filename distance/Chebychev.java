package distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 30 2015
 */
public class Chebychev implements DistanceMeasure {
	

	public Chebychev() {
		super();
	}
	

	@Override
	public double distanceOperator(double partialResult, double addResult) {
		
		return Math.max(partialResult, addResult);
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
		return "Chebychev";
	}


	@Override
	public double computePartialDistance(Parameters param) {
		
		return Math.abs(param.getC1()-param.getC2());

	}
	
	public boolean isCompatibile(String pattern){
		
			return true;
	}

}
