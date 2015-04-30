package distance.d2;

import java.util.Map;

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
public class EstimatedProbD2Star implements DistanceMeasure, EstimatedProb {

	@Override
	/**
	 * Calcola la dissimilarità parziale per due sequenze
	 * secondo la formula D2Star con la probabilità osservata
	 * dei caratteri nelle singole sequenze
	 * Partial_D2S=(c1tilde*c2tilde)/(Math.sqrt(n*m*Pwx*Pwy))
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
		
		String kmer = param.getKmer();
		
		//mappe che contengono il conto effettivo delle lettere nelle due sequenze
		Map<String, Integer> map1 = param.getMapS1();
		Map<String, Integer> map2 = param.getMapS2();
		
		double n = length1-k+1;
		double m = length2-k+1;
		
		//calcolo le probabilità stimate del kmer sulle singole sequenze
		double Pwx = computePw(kmer, k, length1, map1);
		double Pwy = computePw(kmer, k, length2, map2);
		if(Constant.DEBUG_MODE)
			System.out.println("Pwx e Pwy "+Pwx+" "+Pwy);
		
		double c1tilde = c1-n*Pwx;
		double c2tilde = c2-m*Pwy;
		
		double upper=c1tilde*c2tilde;
		double lower=Math.sqrt(n*m*Pwx*Pwy);
		double toreturn;
		if(upper==0&&lower==0)
			toreturn=0;
		else
			toreturn=upper/lower;
		if(Constant.DEBUG_MODE){
			System.out.println("upper/lower: "+upper+" "+lower+" to return: "+toreturn);
			System.out.println("");
		}
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
		return "EstimatedProbD2Star";
	}
	
	/**Calcola probabilità stimata del kmer osservata sulla singola sequenza
	 * (ogni lettera ha probabilità pl/length)
	 * 
	 * @param kmer il kmer considerato
	 * @param k lunghezza del kmer
	 * @param length lunghezza della sequenza considerata
	 * @param map Conto kmer prima sequenza (per recuperare conto della singola lettera)
	 * @return probabilità stimata del kmer nella sequenza considerata
	 */
	private double computePw(String kmer, int k, int length, Map<String, Integer> map){
		double pl=0; //probabilità osservata di una singola lettera nella sequenza
		double Pw=1;	//probabilità dell'intero kmer
		String currletter;
		for(int i=0;i<k;i++){
			currletter=""+kmer.charAt(i);
			if(!map.containsKey(currletter))
				pl=0;
			else
				pl=((double)map.get(currletter))/length;
			Pw*=pl;
		}
		return Pw;
	}
	
	public  boolean isCompatibile(String pattern){
		if(!pattern.contains("0") && pattern.length()>1)
			return true;
		else
			return false;
	}
	
}
