

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
* Implements the subsequent Jobs reduce functionality for PageRankMain.java
* @author Alice, Spencer, Garth
*
*/
public class PageRankReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	/** Overrides reduce
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		// Set up variables
		long sinks = context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue();
		double redistributeValue = sinks/(CONST.TOTAL_NODES * CONST.SIG_FIG_FOR_TINY_DOUBLE_TO_LONG);
		double newPageRank = CONST.RANDOM_SURFER*CONST.BASE_PAGE_RANK + CONST.DAMPING_FACTOR * redistributeValue;
		double oldPageRank = 0.;
		String toList = "";
		
		// Loop through values and handle multiple edged nodes vs single edge nodes
		for (Text val : vals){
			if (val.toString().contains(CONST.L0_DIV)){
				String[] info = val.toString().split(CONST.L0_DIV, -1);
				toList = info[0];
				oldPageRank = Double.parseDouble(info[1]);
			} else {
				newPageRank += CONST.DAMPING_FACTOR * Double.parseDouble(val.toString());
			}
			
		}
		
		// Calculate residual
		double residualValue = Math.abs(newPageRank - oldPageRank)/newPageRank;
		context.getCounter(PageRankEnum.RESIDUAL_SUM).increment((long)(residualValue * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
		
		// Write out key, with to list and updated PR and residual
		try {
			context.write(key, new Text(toList + CONST.L0_DIV + newPageRank + CONST.L0_DIV + residualValue));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
