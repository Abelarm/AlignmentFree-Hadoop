package CV.distance;
//CLasse che calcola la distanza JSD con log in base 2 e normalizzazione
public class JSDLog2WithFrequencies extends JSDLog2 {

	public JSDLog2WithFrequencies(){super();}


	/*@Override
	public double computePartialDistance(int c1, int length1, int c2,int length2, int k) {

		double[] v=DistanceMeasure.getNormalizedValues(c1, length1, c2, length2, k);
		double dkl1,dkl2;
		dkl1=DKL(v[0],v[1]);
		dkl2=DKL(v[1],v[0]);

		return ((0.5)*dkl1) +((0.5)*dkl2) ;
	}*/
	
	@Override
	public double computePartialDistance(Parameters param) {

		double[] v=DistanceMeasure.getNormalizedValues(param.getC1(), param.getLength1(), param.getC2(), param.getLength2(), param.getK());
		double dkl1,dkl2;
		
		    String k=param.getKmer();
	        double c3 = (v[0]+v[1])/2;
			dkl1 = (0.5) * DKL(v[0], c3);
			dkl2 = (0.5) * DKL(v[1], c3);
			double somma=dkl1+dkl2;
			return ( dkl1) + ( dkl2);
		}
	
	@Override
	public String getName(){

		return "JSDLog2WithFrequencies";

	} 
}
