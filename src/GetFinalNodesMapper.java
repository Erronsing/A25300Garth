import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Implements the last Job map functionality for All main functions
 * @author Alice, Spencer, Garth
 *
 */
public class GetFinalNodesMapper extends
		Mapper<LongWritable, BytesWritable, LongWritable, Text> {
	
/** Overrides map
 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
 */
public void map(LongWritable keyin, BytesWritable val, Context context){
		
		// Separate Data and fill nodes HashMap
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		Util.fillBlockFromByteBuffer(ByteBuffer.wrap(val.getBytes()), nodes, null, null);

		// Get data and write out for first two nodes in each block
		for (Node n : nodes.values()){
			if (Util.isInFirstTwoIndex((int) keyin.get(), n.id)){
				try {
					context.write(new LongWritable(n.id), new Text(n.getPR()+ ""));
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	
	}
}

