package quadIndex;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

public class QuadTreeTest {
	public static void main(String args[]) throws IOException {

		InputStream fis;
		BufferedReader br;
		//String line;

		fis = new FileInputStream("/Users/miaodonghan/Downloads/Lakes.txt");
		br = new BufferedReader(new InputStreamReader(fis));

		//QuadTree quad = new QuadTree();
		//long l = 0L;
	/*	while ((line = br.readLine()) != null) {
			SpatialObj o = InputParser.getObjFromLine(line);
			quad.insert(o,l++);
		}*/

		// Done with the file
		br.close();
		br = null;
		fis = null;
		//Set<FileLoc> s = quad.cur_RangeQuery(new Rectangle(0,0,1000000,1000000));
		//System.out.println(s.size());
		
		/*
		String regFilter = ".*part-\\d+$";
		String path = "sad/sdf/part-00000";
	    Pattern pattern = Pattern.compile(regFilter);
	 
	       Matcher m = pattern.matcher(path);
	        
	        System.out.println("Is path : " + path + " matching "
	            + regFilter + " ? , " + m.matches());
	        System.out.print( m.matches());
	    
		*/
		byte b[] = {0,0,0,0,0,0,0,1};
		ByteBuffer bb = ByteBuffer.wrap(b);
		for(int i =0;i<2;i++)
		{
			System.out.println(bb.getInt());
		}
		
		
		/*
		 * Random rand = new Random(); QuadTree quad = new QuadTree(); for(int i
		 * = 0; i< 34530; i++) { Point p = new
		 * Point(rand.nextInt(100000),rand.nextInt(100000)); quad.insert(p); }
		 * //System.out.println(quad.Count()); Set<Integer> s1 =
		 * quad.RangeQuery(new Rectangle(0,0,50000,50000)); Set<Integer> s2 =
		 * quad.RangeQuery(new Rectangle(0,50000,50001,50001)); Set<Integer> s3
		 * = quad.RangeQuery(new Rectangle(50000,0,50001,50001)); Set<Integer>
		 * s4 = quad.RangeQuery(new Rectangle(50000,50000,50001,50001));
		 * 
		 * System.out.println(s1.size()+s2.size()+s3.size()+s4.size());
		 * 
		 * //quad.DebugOutput();
		 */
	}
}
