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
public class UniformProbD2Star implements DistanceMeasure, UniformProb {

	@Override
	/**
	 * Calcola la dissimilarità parziale per due sequenze
	 * secondo la formula D2Star con probabilità uniforme
	 * dei caratteri 
	 * Partial_D2Star=(c1tilde*c2tilde)/(Math.sqrt(n*m)*Pw)
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
		
		double Pw = computePw(k, numlettere);
		double c1tilde = c1-n*Pw;
		double c2tilde = c2-m*Pw;
		
		double upper=c1tilde*c2tilde;
		double lower=Math.sqrt(n*m)*Pw; //dato che Pw è uniforme, va messa fuori (o sotto radice come Pw*Pw)
		double toreturn;
		//per convenzione 0/0 viene posto a 0
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
		return "UniformProbD2Star";
	}
	
	/**Calcola la probabilità di un qualsiasi kmer di lunghezza k
	 * (perchè le probabilità di ogni singola lettera sono uniformi)
	 * 
	 * @param k lunghezza del kmer
	 * @param numlettere numero di lettere che compongono l'alfabeto
	 * @return probabilità di un kmer di lunghezza k
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