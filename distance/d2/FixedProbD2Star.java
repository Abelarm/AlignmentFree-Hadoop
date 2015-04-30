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
public class FixedProbD2Star implements DistanceMeasure, FixedProb {

	@Override
	/**
	 * Calcola la dissimilarità parziale per due sequenze
	 * secondo la formula D2Star con la probabilità
	 * dei caratteri stabilita dall'utente
	 * Partial_D2Star=(c1tilde*c2tilde)/(Math.sqrt(n*m*Pwx*Pwy)
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
		
		//mappa che contiene le probabilità fissate dall'utente
		Map<String, Double> probMap1 = param.getProbMap1();
		Map<String, Double> probMap2 = param.getProbMap2();
		
		double n = length1-k+1;
		double m = length2-k+1;
		
		double Pwx = computePw(kmer, k, probMap1);
		double Pwy = computePw(kmer, k, probMap2);
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
		//if(Constant.DEBUG_MODE)
			
		return toreturn;
	}

	@Override
	/**
	 * Applica "l'operazione esterna" per calcolare il risultato
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
		return "FixedProbD2Star";
	}
	
	/**
	 * Calcola probabilità del kmer in base alle probabilità
	 * delle singole lettere passate in input dall'utente
	 * 
	 * @param kmer il kmer considerato
	 * @param k lunghezza del kmer
	 * @param length lunghezza della sequenza considerata
	 * @param probmap mappa delle probabilità delle lettere nella sequenza
	 * @return probabilità del kmer nella sequenza considerata
	 */
	private double computePw(String kmer, int k, Map<String, Double> probMap){
		double pl=0; //probabilità di una singola lettera
		double Pw=1;	//probabilità dell'intero kmer
		for(int i=0;i<k;i++){
			String currletter=""+kmer.charAt(i);
			if(!probMap.containsKey(currletter.toUpperCase()))
				pl=0;
			else
				pl=probMap.get(currletter.toUpperCase());
			
			Pw*=pl;
		}
		return Pw;
	}
	public  boolean isCompatibile(String pattern){
		if(!pattern.contains("0"))
			return true;
		else
			return false;
	}
	
}
