package CV.distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 30 2015
 */
public class NullDistance implements DistanceMeasure {
	

	public NullDistance() {
		super();
	}

	@Override
	public double computePartialDistance(Parameters param) {
				
		return 0.0;
	}
	
	@Override
	public double distanceOperator(double partialResult, double addResult) {
		return 0.0;
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
		return 0.0;
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
		return "NullDistance";
	}

	public boolean isCompatibile(String pattern){
			return true;
		
	}
}
