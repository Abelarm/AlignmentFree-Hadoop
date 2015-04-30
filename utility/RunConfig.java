package utility;
import java.util.HashMap;


public class RunConfig {

	static HashMap<String, String> costanti = new HashMap<String, String>();

	public static void setValue(String nome, String valore) {
		costanti.put(nome, valore);
	}
	
	public static String getValue(String nome){
		return costanti.get(nome);
	}
	
}
