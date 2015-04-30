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


//CLasse che calcola la distanza JSD con log naturale 
public class JSDLogN extends KLDLogN {


	public JSDLogN(){ super();}

//	// METODO PER CALCOLARE I DKL CHE SERVONO AL METODO computePartialDistance
//	// p e q sono i contatori dei kmers che passiamo (prima c1,c2 poi c2,c1).
//	// si usa il logaritmo naturale
//	@Override
//	public double DKL(double p, double q) {
//		double dis = 0.0;
//		if (p != 0)
//			dis = p * (Math.log(p / (0.5 * (p + q))));
//
//		return dis;
//	}
	@Override
	public String getName() {
		return "JSDLogN";
	}
	public boolean isSymmetricMeasure() {
		return true;
	}
	
}
