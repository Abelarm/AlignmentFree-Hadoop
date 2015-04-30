package distance;

/**
 * 
 * @author Francesco Gaetano - email: f.gaetano90@gmail.com
 * @author Luigi Lomasto - email: luigilomasto@gmail.com 
 * 
 * @version 1.1
 * 
 * Date: February, 15 2015
 */


public class CoPhylogDistance implements ContextObject {

	@Override
	public double computePartialDistance(Parameters param) {
		
		
		String c1 = param.getContesto1();
		String c2 = param.getContesto2();
		
		String o1 = param.getOggetto1();
		String o2 = param.getOggetto2();
		
		//System.out.println(o1+"   "+o2);
		if(c1.equals(c2) && (!o1.equals(o2)))
			return 1;
		else 
			return 0;
		
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
		
		return false;
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
		
		return "Co-Phylog";
	}
	
	public  boolean isCompatibile(String pattern){
		if(pattern.contains("0"))
			return true;
		else
			return false;
	}
	
}