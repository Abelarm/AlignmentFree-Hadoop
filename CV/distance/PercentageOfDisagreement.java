package CV.distance;

public class PercentageOfDisagreement implements  DistanceMeasure{

	@Override
	public boolean hasInternalProduct() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public double computePartialDistance(Parameters param) {
		double c1 = param.getC1();
		double c2 = param.getC2();
		
		if(c1!=c2)
			return 1;
		else
			return 0;
	}

	@Override
	public double distanceOperator(double partialDist, double currDist) {
		
		return partialDist + currDist;
	}

	@Override
	public double initDistance() {
		return 0;
	}

	@Override
	public double finalizeDistance(double dist, int numEl) {
		
		return dist/numEl;
	}

	@Override
	public boolean isSymmetricMeasure() {
		return true;
	}

	@Override
	public String getName() {
		return "PercentageOfDisagreement";
	}

	public boolean isCompatibile(String pattern){
		if(pattern.contains("0"))
			return true;
		else
			return false;
	}
}
