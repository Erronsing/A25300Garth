import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Implements the first Job reduce functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class BlockReducerPass0 extends
		Reducer<LongWritable, Text, LongWritable, BytesWritable> {
	/** Overrides the Reduce function
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(LongWritable key, Iterable<Text> vals, Context context){
		
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		int max = Util.baseValue((byte) (key.get() + 1));
		for (int i = Util.baseValue((byte) key.get()); i < max; i++){
			Node n = new Node(i);
			n.setPR(CONST.BASE_PAGE_RANK);
			nodes.put(i, n);
		}
			
		HashMap<Integer, ArrayList<Edge>> i = new HashMap<Integer, ArrayList<Edge>>();

		HashMap<Integer, ArrayList<Edge>> o = new HashMap<Integer, ArrayList<Edge>>();
		
		
		// Loops through all the values associated with the key
		for (Text val : vals){
			
			// Each value is checked if we have seen it, seen the edge, and add the node
			// to seenNodes and nodes appropriately
			String[] info = val.toString().split(CONST.L0_DIV, -1);
			byte marker = Byte.parseByte(info[0]);
			if (marker == CONST.SEEN_EDGE_MARKER){
				int from = Integer.parseInt(info[1]);
				int to = Integer.parseInt(info[2]);
				nodes.get(from).addBranch();
				// Specifies if we are currently looking at an internal edge or external edge for a block
				if (Util.idToBlock(to) == key.get()){
					if (i.containsKey(to)){
						i.get(to).add(new Edge(from, to));
					} else {
						ArrayList<Edge> ae = new ArrayList<Edge>();
						ae.add(new Edge(from, to));
						i.put(to,  ae);
					}
					
				} else {
					if (o.containsKey(to)){
						o.get(to).add(new Edge(from, to));
					} else {
						ArrayList<Edge> ae = new ArrayList<Edge>();
						ae.add(new Edge(from, to));
						o.put(to,  ae);
					}
				}
				
			}
			
		}
		// Add dividers as necessary for empty strings
		
		
		try {
			// Write out block data for next job
			context.write(key, new BytesWritable(Util.blockToByteBuffer(nodes, i, o).array()));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		 
	}


}
