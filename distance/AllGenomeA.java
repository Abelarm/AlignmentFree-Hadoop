package distance;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import utility.Util;

public class AllGenomeA {
	
	public static HashMap<String, Double> calcolaA(String genome, String idSeq, int k){
		HashMap<String, Double> vector = new HashMap<String, Double>();
		HashMap<String, Double> count = new HashMap<String, Double>();
		HashMap<String, Double> count2 = new HashMap<String, Double>();
		String kmer,kmer1,kmer1b,kmer2;
		double p,p11,p11b,p12,p0,a;
		int positive=0;
		int length = 0;
		if(k<3)
			return null;
		try{
			
			String allStrGenome = genome.trim();
			length=allStrGenome.length();
			
			if(allStrGenome.equals("") || !Util.isValidFASTAFormat(allStrGenome))
				return null;
			int newK=k;
			for(int j = 0;j<3;j++){
				newK=k-j;
				/* cycle over the length of String till k-mers of length, k, can still be made */
				for(int i = 0; i< (allStrGenome.length()-newK+1); i++){
					/* output each k-mer of length k, from i to i+k in String*/
					
					kmer = allStrGenome.substring(i, i+newK);
					
					double val;
					if(j==0){
						if(count.get(kmer)==null)
							count.put(kmer, 1.0);
						else{
							val=count.get(kmer);
							val++;
							count.put(kmer, val);
						}
					}
					else{
						if(count2.get(kmer)==null)
							count2.put(kmer, 1.0);
						else{
							val=count2.get(kmer);
							val++;
							count2.put(kmer, val);
						}
					}
					
					
				}
			}
			
			Iterator<Entry<String,Double>> it = count.entrySet().iterator();
			Entry<String,Double> val;
			while(it.hasNext()){
				val=it.next();
				kmer=val.getKey();
				kmer1=kmer.substring(0, k-1);
				kmer1b=kmer.substring(1, k);
				kmer2=kmer.substring(1, k-1);
				
				p=val.getValue()/(length-k+1);
				p11=count2.get(kmer1)/(length-(k-1)+1);
				p11b=count2.get(kmer1b)/(length-(k-1)+1);
				p12=count2.get(kmer2)/(length-(k-2)+1);
				
				/*if(p12!=0){
					p0=(p11*p11b)/p12;
					a=(p-p0)/p0;
						positive++;
				}
				else
					a=0;*/
				if(p12==0)
					p0=0;
				else
					p0=(p11*p11b)/p12;
				a=(p-p0)/p0;
				
				System.out.println(kmer+" [idSeq="+idSeq+", count="+a);
				vector.put(kmer, a);
			}
			return vector;
			
		}
		
		
		catch(Exception e){
			e.printStackTrace();
		}
		
		
		return null;
	}
}
