package CV.distance;

import java.util.ArrayList;

public class CSQ implements DistanceMeasure, DistanceMeasureDouble{

	@Override
	public boolean hasInternalProduct() {
		return false;
	}

	@Override
	public double computePartialDistance(Parameters param) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double distanceOperator(double partialDist, double currDist) {
		return 0;
	}

	@Override
	public double initDistance() {
		return 0;
	}

	@Override
	public double finalizeDistance(double dist, int numEl) {
		return 0;
	}

	@Override
	public boolean isSymmetricMeasure() {
		return true;
	}

	@Override
	public String getName() {
		return "CSQ";
	}

	@Override
	public boolean isCompatibile(String pattern) {
		if(pattern.contains("0")||pattern.length()<3)
			return false;
		return true;
	}

	@Override
	public ArrayList<Double> computePartialDistanceDouble(Parameters param) {
		double a1 = param.getC1();
		double a2 = param.getC2();
		ArrayList<Double> partial = new ArrayList<Double>();
		partial.add(0,(a1*a2));
		partial.add(1,(a1*a1));
		partial.add(1,(a2*a2));
		return partial;
	}

	@Override
	public ArrayList<Double> distanceOperatorDouble(
			ArrayList<Double> partialDist, ArrayList<Double> currDist) {
		double top = partialDist.get(0)+currDist.get(0);
		double down1 = partialDist.get(1)+currDist.get(1);
		double down2 = partialDist.get(2)+currDist.get(2);
		ArrayList<Double> toReturn = new ArrayList<Double>();
		toReturn.add(0,top);
		toReturn.add(1, down1);
		toReturn.add(2, down2);
		return toReturn;
	}

	@Override
	public double finalizeDistanceDouble(ArrayList<Double> dist, int numEl) {
		double CSQ = dist.get(0)/(Math.sqrt(dist.get(1)*dist.get(2)));
		return (1-CSQ)/2;
	}

	
}
