package CV.distance;


/**
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.4
 * 
 * Date: January, 26 2015
 */
public interface DistanceMeasure{

	/**
	 * 
	 * @return
	 */
	public boolean hasInternalProduct(); //restituisce true se bisogna fare il prodotto tra i due operandi.

	/**
	 * 
	 * @param param
	 * @return
	 */
	public double computePartialDistance(Parameters param); 
	
	
	/**
	 * 
	 * @param partialDist
	 * @param currDist
	 * @return
	 */
	//Puo' essere la somma o il prodotto a secondo del tipo di distanza usata.
	public double distanceOperator(double partialDist, double currDist);

	/**
	 * 
	 * @return
	 */
	//Puo' essere 0 per la somma o 1 per il prodotto a secondo del tipo di distanza usata.
	public double initDistance();//return 0 per la somma e 1 per il prodotto.
	//TODO Vedere se metterlo in combiner.

	/**
	 * 
	 * @param dist
	 * @return
	 */
	public double finalizeDistance(double dist, int numEl); //Es. se devo calcolare una media, qui faccio la divisione.

	/**
	 * 
	 * @return
	 */
	public boolean isSymmetricMeasure();

	/**
	 * 
	 * @return
	 */
	public String getName();
	
	public boolean isCompatibile(String pattern);


	/**
	 * 
	 * @param c1
	 * @param length1
	 * @param c2
	 * @param length2
	 * @param k
	 * @return
	 */
	public static double[] getNormalizedValues(double c1, int length1, double c2, int length2, int k){

		double[] v = new double[]{0.0, 0.0};

		int den1 = length1 - k + 1;
		int den2 = length2 - k + 1;

		if(c1!=0 && length1!=0 && den1>0)
			v[0] = (double) c1/den1;

		if(c2!=0 && length2!=0 && den2>0)
			v[1] = (double) c2/den2;

		return v;

	}

}
