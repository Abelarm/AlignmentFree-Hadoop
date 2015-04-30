package utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ParserRose {

	public static void Parse(String FileRose,String newFilePath) { //TODO

	
		System.out.println("QUI "+FileRose);
		
		
		

		try {
			Scanner in = new Scanner(new File(FileRose));
			PrintWriter writer = new PrintWriter(newFilePath+".fasta");
			
			String currLine;
			while(in.hasNextLine()){
				currLine = in.nextLine();
				if(currLine.contains("Sequences:"))
					continue;
				if(currLine.equals("")){
					continue;
				}
				if(currLine.contains("Alignment:"))
					break;
				System.out.println(currLine);
				writer.println(currLine);
			}
			writer.close();
			

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
