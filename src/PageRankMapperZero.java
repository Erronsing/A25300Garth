import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
* Implements the first Job map functionality for PageRankMain.java
* @author Alice, Spencer, Garth
*
*/
public class PageRankMapperZero extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	
	/** Overrides map
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable keyin, Text val, Context context){
		// Process modified edges.txt in preparation for mapreduce
		String[] info = val.toString().split(" ");
		try {
			// Get data and set to variables 
			double select = Double.parseDouble(info[0]);
			LongWritable fromInt = new LongWritable(Integer.parseInt(info[1]));
			LongWritable toInt = new LongWritable(Integer.parseInt(info[2]));
			Text toText = new Text(info[2]);
			Text nullTo = new Text("-1");
			// Check to see if we keep the edge based on netid
			if (Util.retainEdgeByNodeID(select)){ // If we keep it, write edge out (from node, to node)
				try {
					context.write(fromInt, toText);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else  { // else keep node so we don't lose it, but have it point to null node
				try {
					context.write(fromInt, nullTo);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try { // To ensure we don't lose a node, write out nodeTo with no edges 
					context.write(toInt, nullTo);
			} catch (IOException | InterruptedException e) {
				System.out.println("error2");
				e.printStackTrace();
			}
		} catch (NumberFormatException e) {
			System.out.println("error1");
			e.printStackTrace();
		}
	}
}
