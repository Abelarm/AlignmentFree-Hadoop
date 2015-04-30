package CV.distance;
//CLasse che calcola la distanza JSD con log in base 2 
//Jensen-Shannon Divergence
public class JSDLog2  extends KLDLog2 implements DistanceMeasure {

	// METODO PER CALCOLARE I DKL CHE SERVONO AL METODO computePartialDistance
	// p e q sono i contatori dei kmers che passiamo (prima c1,c2 poi c2,c1).
	// si usa il logaritmo in base 2

	@Override
	public double computePartialDistance(Parameters param) {

		double dkl1, dkl2;
        double c3 = (param.getC1() + param.getC2())/2;
		dkl1 = (0.5) * DKL(param.getC1(), c3);
		dkl2 = (0.5) * DKL(param.getC2(), c3);
		return ( dkl1) + ( dkl2);
	}

	@Override
	public boolean isSymmetricMeasure() {
		return true;
	}

	@Override
	public String getName() {
		return "JSDLog2";
	}
	
	public boolean isCompatibile(String pattern){
		
			return true;
		
	}
	

}
