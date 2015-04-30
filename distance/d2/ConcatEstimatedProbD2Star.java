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
public class ConcatEstimatedProbD2Star implements DistanceMeasure, EstimatedProb {

	@Override
	/**
	 * Calcola la dissimilarità parziale per due sequenze
	 * secondo la formula D2Star con la probabilità osservata
	 * dei caratteri nella concatenazione delle due sequenze
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
		String kmer = param.getKmer();
		
		//mappe contenenti il conto dei vari kmer
		Map<String, Integer> map1 = param.getMapS1();
		Map<String, Integer> map2 = param.getMapS2();
		
		double n = (length1-k)+1;
		double m = (length2-k)+1;
		
		//calcolo la probabilità stimata del kmer sulla concatenazione delle sequenze
		double Pw = computePw(kmer, k, length1, length2, map1, map2);
		if(Constant.DEBUG_MODE)
			System.out.println("Pw "+Pw);
		
		double c1tilde = c1-n*Pw;
		double c2tilde = c2-m*Pw;
		
		double upper=c1tilde*c2tilde;
		double lower=Math.sqrt(n*m)*Pw;
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
		return "ConcatEstimatedProbD2Star";
	}
	
	/**Calcola probabilità stimata del kmer osservata sulla concatenazione delle sequenze
	 * (ogni lettera ha probabilità (pl1+pl2)/totlen )
	 * 
	 * @param kmer il kmer considerato
	 * @param k lunghezza del kmer
	 * @param length1 lunghezza prima sequenza
	 * @param length2 lunghezza seconda sequenza
	 * @param map1 Conto kmer prima sequenza (per recuperare conto della singola lettera)
	 * @param map2 Conto kmer seconda sequenza (per recuperare conto della singola lettera)
	 * @return probabilità stimata del kmer sulla concatenazione
	 */
	private double computePw(String kmer, int k, int length1, int length2, Map<String, Integer> map1, Map<String, Integer> map2){
		double totlen=length1+length2;
		double pl=0; //probabilità osservata di una singola lettera nella concatenazione delle sequenze
		double Pw=1;	//probabilità dell'intero kmer
		double pl1, pl2; //probabilità osservata di una singola lettera nelle due sequenze
		for(int i=0;i<k;i++){
			String currletter=""+kmer.charAt(i);
			if(!map1.containsKey(currletter))
				pl1=0;
			else
				pl1=((double)map1.get(currletter))/totlen;
			if(!map2.containsKey(currletter))
				pl2=0;
			else
				pl2=((double)map2.get(currletter))/totlen;
			pl+=pl1;
			pl+=pl2;
			Pw*=pl;
			pl=0;
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
