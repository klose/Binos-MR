package temporary;
import java.util.ArrayList;


public class testOutput<KEY, VALUE> {
	public static void main(String[] args) {
		String a = "MSG:/10.5.0.170:2ada2222";
		if (a.matches("MSG://.*")) {
			System.out.println("right");
		}
	}
	
}
