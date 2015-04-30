package CV.distance;
// CLasse che calcola la distanza JSD con log naturale e normalizzazione
public class JSDLogNWithFrequencies extends JSDLogN{


	public JSDLogNWithFrequencies(){super();}

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
		dkl1=DKL(v[0],v[1]);
		dkl2=DKL(v[1],v[0]);

		return ((0.5)*dkl1) +((0.5)*dkl2) ;
	}
	
	@Override
	public String getName() {
		return "JSDLogNWithFrequencies";
	}
}
