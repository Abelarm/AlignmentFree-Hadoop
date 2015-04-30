package distance;

import java.util.ArrayList;
import java.util.Map;

public class CVInit {
	
	static ArrayList<Double> CV1 = new ArrayList<Double>();
	static ArrayList<Double> CV2 = new ArrayList<Double>();
	
	public static void addCV(Parameters param){
		int c1 = param.getC1();
		int c2 = param.getC2();
		int length1 = param.getLength1();
		int length2 = param.getLength2();
		Map<String, Integer> map1 = param.getMapS1();
		Map<String, Integer> map2 = param.getMapS2();
		int k = param.getK();
		String kmer = param.getKmer();
		double p_kmer_1 = (double)c1/(length1-k+1);
		double p_kmer_2 = (double)c2/(length2-k+1);
		String stringa = kmer.substring(0, k-1);
		int c11 = getC(stringa, map1);
		int c21 = getC(stringa, map2);
		stringa = kmer.substring(1, k);
		int c12 = getC(stringa, map1);
		int c22 = getC(stringa, map2);
		stringa = kmer.substring(1, k-1);
		int c13 = getC(stringa, map1);
		int c23 = getC(stringa, map2);
		double p01;
		double p02;
		if(c13==0)
			p01=0;
		else
			p01 = getP0(c11, c12, c13);
		if(c23==0)
			p02=0;
		else
			p02 = getP0(c21, c22, c23);
		CV1.add(getA(p_kmer_1, p01));
		CV2.add(getA(p_kmer_2, p02));
	}

	private static int getC(String kmer, Map<String, Integer> map){
		if(map.containsKey(kmer))
			return map.get(kmer);
		else
			return 0;
	}
	
	private static double getP0(int c_1, int c_2, int c_3){
		return (double)(c_1*c_2)/c_3;
	}
	
	private static double getA(double p_kmer, double p0){
		if(p0==0)
			return 0;
		else
			return (p_kmer-p0)/p0;
	}
	
	public static ArrayList<Double> getCV(int seqnum){
		if(seqnum==1)
			return CV1;
		else
			return CV2;
	}
}
