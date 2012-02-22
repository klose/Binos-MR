package temporary;
import java.util.ArrayList;


public class testOutput<KEY, VALUE> {
	public static void main(String[] args) {
		String a = "MSG:/10.5.0.170:2ada2222";
		if (a.matches("MSG://.*")) {
			System.out.println("right");
		}
		String workDir = "/a/bc/1111";
		String taskid = workDir.substring(workDir.lastIndexOf("/") +1); 
		System.out.println(taskid);
	}
	
}
