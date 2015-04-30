package distance.d2;


import utility.Constant;
import distance.DistanceMeasure;
import distance.Parameters;

/**
 * 
 * @author Luigi Giugliano - Steven Rosario Sirchia 
 * 
 * @version 1.1
 * 
 * Date: February, 2 2015
 */
public class UniformProbD2S implements DistanceMeasure, UniformProb {

	@Override
	/**
	 * Calcola la dissimilarità parziale per due sequenze
	 * secondo la formula D2S con la probabilità
	 * dei caratteri uniforme su entrambe le sequenze
	 * Partial_D2S=(c1tilde*c2tilde)/(Math.sqrt(c1tilde^2+c2tilde^2))
	 * @param param, wrapper dei parametri
	 * @return dissimilarità parziale
	 */
	public double computePartialDistance(Parameters param) {
		//recupero i parametri
		int c1 = param.getC1();
		int c2 = param.getC2();
		
		int length1 = param.getLength1();
		int length2 = param.getLength2();
		int k = param.getK();
		
		double n = length1-k+1;
		double m = length2-k+1;
		int numlettere = param.getNumLettere();
		
		//calcolo probabilità del kmer
		double Pw = computePw(k, numlettere);
		
		double c1tilde = c1-n*Pw;
		double c2tilde = c2-m*Pw;
		
		double upper=c1tilde*c2tilde;
		double lower=Math.sqrt((c1tilde*c1tilde)+(c2tilde*c2tilde));
		double toreturn;
		if(upper==0&&lower==0)
			toreturn=0;
		else
			toreturn=upper/lower;
		if(Constant.DEBUG_MODE)
			System.out.println("upper/lower: "+upper+" "+lower+" to return: "+toreturn);
		return toreturn;
	}

	@Override
	/**Applica "l'operazione esterna" per calcolare il risultato
	 * 
	 * @param partialResult risultato parziale
	 * @param addResult risultato totale accumulato finora
	 * @return nuovo risultato totale intermedio
	 */
	public double distanceOperator(double partialResult, double addResult) {
		return partialResult + addResult;
	}

	@Override
	/**
	 * Inizializza la distanza a 0
	 */
	public double initDistance() {
		return 0;
	}

	@Override
	/**
	 * Indica che ha un prodotto interno
	 */
	public boolean hasInternalProduct() {
		return false;
	}

	@Override
	/**
	 * Restituisce la distanza
	 */
	public double finalizeDistance(double dist, int numEl) {
		return dist;
	}

	@Override
	/**
	 * Indica che la distanza è simmetrica
	 */
	public boolean isSymmetricMeasure() {
		return true;
	}

	@Override
	/**
	 * Restituisce il nome della classe
	 */
	public String getName() {
		return "UniformProbD2S";
	}
	
	/**Calcola probabilità di un qualsiasi kmer
	 * di lunghezza k (perchè sono probabilità uniformi)
	 * 
	 * @param k lunghezza del kmer
	 * @param numlettere
	 * @return probabilità di un qualsiasi kmer di lunghezza k
	 */
	private double computePw(int k, int numlettere){
		double Pw=(double)k*(1.0/numlettere);
		return Pw;
	}
	
	public  boolean isCompatibile(String pattern){
		if(!pattern.contains("0"))
			return true;
		else
			return false;
	}
}