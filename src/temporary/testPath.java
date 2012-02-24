package temporary;

import org.apache.hadoop.fs.Path;

public class testPath {
	public static void main(String[] args) {
		Path p = new Path("FILE://10.5.0.170:26666/uesr/jiangbing/input");
//		Path p = new Path("MSG://132434354-asda");
		System.out.println(p.toString());
		//System.out.println(p.toUri().getPath());
	}
}
