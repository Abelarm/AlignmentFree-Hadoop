package utility;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.*;

import org.w3c.dom.*;
import org.xml.sax.*;

public class XMLConfigParser{
 
       //main di test
       public static void main(String[] args) {
             //String filename = "costanti.xml";
    	     String filename = "src/test/st1/PropostaXMLConstants/myconfig.xml";
    	     //String filename = "src/test/st1/PropostaXMLConstants/errorconfig.xml";
             parseConstants(filename);
             System.out.println("valore: "+RunConfig.getValue("CHECK_INPUT"));
             System.out.println("valore: "+RunConfig.getValue("DEBUG_MODE"));
             System.out.println("valore: "+RunConfig.getValue("BUFFER_CAPACITY_SEQ"));
             System.out.println("valore: "+RunConfig.getValue("GENERATE_INPUT"));
       }
 
       public static HashMap<String, String> readXML(String filename){
    	   parseConstants(filename);
    	   return RunConfig.costanti;
       }
       /**
        * Parsa il file di configurazione di nome filename
        * per recuperare le costanti di configurazione
        * @param filename
        */
       private static void parseConstants(String filename){
    	   XMLConfigParser ddp = new XMLConfigParser();
           DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

           try {
                  DocumentBuilder builder = dbf.newDocumentBuilder();
                  File xmlFile;
                  Document document;
                  try{
                	  xmlFile = new File(filename);
                	  document = builder.parse(xmlFile);
                  }catch(FileNotFoundException fe){
                	  xmlFile = new File(XMLConfigParser.class.getResource("default_config.xml").getFile());
                	  document = builder.parse(xmlFile);
                  }
                  ddp.getNodeInfo(document);
           } catch (SAXException sxe) {
                  Exception  x = sxe;
                  if (sxe.getException() != null)
                         x = sxe.getException();
                  x.printStackTrace();
           } catch (ParserConfigurationException pce) {
                  pce.printStackTrace();
           } catch (IOException ioe) {
                  ioe.printStackTrace();
           }
       }
       
       /**
        * Inserisce le costanti in XMLConstants
        * @param currentNode il nodo corrente
        */
       private void getNodeInfo(Node currentNode) {
             short sNodeType = currentNode.getNodeType();
             //Se è di tipo Element ricavo le informazioni e le inserisco in constant
             if (sNodeType == Node.ELEMENT_NODE) {
                    String nome = currentNode.getNodeName();
                    String valore = searchTextInElement(currentNode);
                    if (!valore.trim().equalsIgnoreCase("")) {
                           RunConfig.setValue(nome, valore.trim());
                    }
					else
						;//do nothing
             }
             int iChildNumber = currentNode.getChildNodes().getLength();
             //Se non si tratta di una foglia continua l'esplorazione 
             if (currentNode.hasChildNodes()) {
                    NodeList nlChilds = currentNode.getChildNodes();
                    for (int iChild = 0; iChild < iChildNumber; iChild++) {
                           getNodeInfo(nlChilds.item(iChild));
                    }
             }
       }
 
       /**
        * Cerca il contenuto del nodo elementNode
        * @param elementNode nodo di riferimento
        * @return valore del'elemento
        */
       private static String searchTextInElement(Node elementNode) {
             String sText = "";
             if (elementNode.hasChildNodes()) {
                    //Il child node di tipo testo è il primo
                    Node nTextChild = elementNode.getChildNodes().item(0);
                    sText = nTextChild.getNodeValue();
             }
             return sText;
       }
}