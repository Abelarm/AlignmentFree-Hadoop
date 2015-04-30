package test;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

import utility.GenerateRandom;

/**
 * Test class.
 * 
 * @author Gianluca Roscigno - email: giroscigno@unisa.it - http://www.di.unisa.it/~roscigno/
 * 
 * @version 1.2
 * 
 * Date: January, 24 2015
 */
public class Test {

	public static void main(String[] args) {
		//System.out.println(GenerateRandom.randomDNAString(10));
		//System.out.println(GenerateRandom.randomProteinString(10));

		//System.out.println(GenerateRandom.randomDnaFastaFile(5, 100, new File("./data/example/random.fasta")));
		//randomNormal(100, 0);
		//System.out.println(GenerateRandom.randomDnaFastaFile(10, 200, 10, new File("./data/example/random.fasta")));
		
		//System.out.println(utility.Util.extractSpacedWord("ATA", "010"));
		//System.out.println(utility.Util.extractPattern("A*G*T*A"));

		//System.out.println(GenerateRandom.randomDnaFastaFile(1, 94371840, new File("/home/user/Scrivania/big.fasta")));
		//System.out.println(GenerateRandom.randomDnaFastaFile(1, 419430400, new File("/home/user/Scrivania/big2.fasta")));
		
		System.out.println(GenerateRandom.randomDnaFastaFile(1, 10485760, new File("/home/user/Scrivania/INPUT/1.fasta")));
		System.out.println(GenerateRandom.randomDnaFastaFile(1, 10465760, new File("/home/user/Scrivania/INPUT/2.fasta")));
		System.out.println(GenerateRandom.randomDnaFastaFile(1, 10484760, new File("/home/user/Scrivania/INPUT/3.fasta")));
		System.out.println(GenerateRandom.randomDnaFastaFile(1, 10485760, new File("/home/user/Scrivania/INPUT/4.fasta")));
		System.out.println(GenerateRandom.randomDnaFastaFile(1, 10485560, new File("/home/user/Scrivania/INPUT/5.fasta")));
	}

	public static void randomNormal(int avg, int sd){
		Random r = new Random();
		ArrayList<Integer> values = new ArrayList<Integer>();

		for(int i=0; i<10; i++){
			//r.nextGaussian(): the next pseudorandom, Gaussian ("normally") distributed double value with mean 0.0 and standard deviation 1.0 from this random number generator's sequence
			//values.add(r.nextGaussian());

			//To generate values with an average of "avg" and a standard deviation of "sd":
			int val = (int) Math.round(r.nextGaussian() * sd + avg);
			values.add(Math.abs(val));
			//values.add(val);
		}

		int sum = 0, max=Integer.MIN_VALUE, min=Integer.MAX_VALUE;

		for(Integer v : values){
			System.out.println(v);
			sum +=v;

			if(v<min)
				min=v;

			if(v>max)
				max=v;
		}

		System.out.println("Average: "+ (double)sum/values.size());
		System.out.println("Min: "+ min);
		System.out.println("Max: "+ max);
	}
}
