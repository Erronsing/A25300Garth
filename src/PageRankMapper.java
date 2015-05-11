

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Implements the subsequent Jobs map functionality for PageRankMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class PageRankMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	/** Overrides map
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable keyin, Text val, Context context){ 
		// Grab values from val
		String[] info = val.toString().split(CONST.L0_DIV, -1);

		// Get To List and PageRank from data
		String[] toList = info[0].split(",");
		Double pr = Double.parseDouble(info[1]);
		
		// If this node has no outgoing edges update sink
		if (toList.length == 0 || (toList.length == 1 && toList[0].equals(""))){
			context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment(
					(long)(pr * CONST.SIG_FIG_FOR_TINY_DOUBLE_TO_LONG + .5)
					);
		} else { // Else Process toList, get nodeID for where the edge goes, and save it with PR value
			for (String to : toList){
				int toID = Integer.parseInt(to);
				try {
					context.write(new LongWritable(toID), new Text(Double.toString(pr/toList.length)));
				} catch (IOException | InterruptedException e) {
					System.out.println("error5");
					e.printStackTrace();
				}
						
			}
		}
		// Write Data out
		try {
			context.write(keyin, new Text(info[0] + CONST.L0_DIV + Double.toString(pr)));
		} catch (IOException | InterruptedException e) {
			System.out.println("error4");
			e.printStackTrace();
		}
			
		
	}
}
