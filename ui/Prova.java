package ui;

import java.util.HashMap;

import utility.RunConfig;
import utility.XMLConfigParser;

public class Prova {

	public static void main(String[] args) {
        //String filename = "costanti.xml";
	     String filename = "data/example/my_config.xml";
	     //String filename = "src/test/st1/PropostaXMLConstants/errorconfig.xml";
	     HashMap<String, String> valori = XMLConfigParser.readXML(filename);
        System.out.println("valore: "+valori.get("CHECK_INPUT"));
        System.out.println("valore: "+valori.get("DEBUG_MODE"));
        System.out.println("valore: "+valori.get("BUFFER_CAPACITY_SEQ"));
        System.out.println("valore: "+valori.get("GENERATE_INPUT"));
  }
}
