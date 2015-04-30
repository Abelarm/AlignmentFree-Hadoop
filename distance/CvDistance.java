package distance;

import java.util.ArrayList;
import java.util.Map;


//composition vector
public class CvDistance implements DistanceMeasure {

	@Override
	public double computePartialDistance(Parameters param) {
		CVInit.addCV(param);
		return 0;
	}
	
	@Override
	public double distanceOperator(double partialResult, double addResult) {
		return 0;
	}

	@Override
	public double initDistance() {
		return 0;
	}

	@Override
	public boolean hasInternalProduct() {
		return false;
	}

	@Override
	public double finalizeDistance(double dist, int numEl) {
		ArrayList<Double> CV1 = CVInit.getCV(1);
		ArrayList<Double> CV2 = CVInit.getCV(2);
		double CSQ= getCSQ(CV1, CV2);
		
		double toreturn=(1-CSQ)*0.25;
		//System.out.println(""+CSQ);
		return toreturn;
	}

	private double getCSQ(ArrayList<Double> CV1,ArrayList<Double> CV2){
		double top=0;
		double down1=0;
		double down2=0;
		
		for(int i=0; i<CV1.size();i++){
			top+=(CV1.get(i)*CV2.get(i));
			down1+=(CV1.get(i)*CV1.get(i));
			down2+=(CV2.get(i)*CV2.get(i));
			//System.out.println("top "+top+" down1 "+down1+" down2 "+down2);
		}
		double down=Math.sqrt(down1*down2);
		//System.out.println("top "+top+" down "+down);
		return top/down;
	}
	
	@Override
	public boolean isSymmetricMeasure() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public String getName() {
		return "CV";
	}
	
	public boolean isCompatibile(String pattern){
		if(!pattern.contains("0"))
			return true;
		else
			return false;
	}
	
}
