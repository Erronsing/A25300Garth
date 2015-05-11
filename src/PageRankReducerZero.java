import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Implements the first Job reduce functionality for PageRankMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class PageRankReducerZero extends Reducer<LongWritable, Text, LongWritable, Text> {

	/** Overrides reduce
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		// Compiles edges into fromID to list of toID
		String toList = "";
		for (Text val : vals){// if nullTo (no edge)
			if (!val.toString().equals("-1"))
				toList += "," + val.toString();
		}
		if (toList.length() > 0){
			toList = toList.substring(1);
		} 
		try { // Write out from -> List of toID, PR
			context.write(key, new Text(toList + CONST.L0_DIV + CONST.BASE_PAGE_RANK));
		} catch (IOException | InterruptedException e) {
			System.out.println("error3");
			e.printStackTrace();
		}
	}
}
