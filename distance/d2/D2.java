package distance.d2;

import distance.DistanceMeasure;
import distance.Parameters;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.1
 * 
 * Date: January, 30 2015
 */
public class D2 implements DistanceMeasure {

	@Override
	public double computePartialDistance(Parameters param) {

		return param.getC1()*param.getC2();
	}
	
	@Override
	public double distanceOperator(double partialResult, double addResult) {
		return partialResult + addResult;
	}

	@Override
	public double initDistance() {
		return 0;
	}

	@Override
	public boolean hasInternalProduct() {
		//Capita che se la distanza tra due sequenze e' 0, in output non c'e' la corrispondente coppia.
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
	public String getName() {
		return "D2";
	}

	@Override
	public String toString() {
		return getName();
	}
	
	public  boolean isCompatibile(String pattern){
		if(!pattern.contains("0"))
			return true;
		else
			return false;
	}

}
