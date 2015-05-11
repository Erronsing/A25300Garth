import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;


public abstract class Util {
	/**
	 * Retain an edge by the value enteres
	 * @param x value provided
	 * @return true if retainable
	 */
	public static boolean retainEdgeByNodeID(double x){
		// compute filter parameters for netid shs257
		double fromNetID = 0.752;// 82 is 28 reversed
		double rejectMin = 0.9 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
	}
	/**
	 * Index of the block this ID falls in the minimum index such that (blocks[id] > id)
	 * @param id id of the node
	 * @return block id
	 */
	public static int idToBlock(int id){
		int guess = id/10078;
		boolean goodGuess = false;
		while (!goodGuess){
			if (guess + 1 < blocks.length && blocks[guess] <= id){
				guess++;
			} else if (guess > 0 && blocks[guess - 1] > id) {
				guess--;
			} else {
				goodGuess = true;
			}
		}
		return guess;
	}
	
	/**
	 * The blocks we use, hardcoded
	 */
	public final static int[] blocks  = {
		10328,
		20373,
		30629,
		40645,
		50462,
		60841,
		70591,
		80118,
		90497,
		100501,
		110567,
		120945,
		130999,
		140574,
		150953,
		161332,
		171154,
		181514,
		191625,
		202004,
		212383,
		222762,
		232593,
		242878,
		252938,
		263149,
		273210,
		283473,
		293255,
		303043,
		313370,
		323522,
		333883,
		343663,
		353645,
		363929,
		374236,
		384554,
		394929,
		404712,
		414617,
		424747,
		434707,
		444489,
		454285,
		464398,
		474196,
		484050,
		493968,
		503752,
		514131,
		524510,
		534709,
		545088,
		555467,
		565846,
		576225,
		586604,
		596585,
		606367,
		616148,
		626448,
		636240,
		646022,
		655804,
		665666,
		675448,
		685230};
	
	/**
	 * The number of nodes that should be in any block,
	 * as defined by blocks[id] - blocks[id - 1]
	 * @param id the id of the block
	 * @return
	 */
	public static int numNodesInBlock(int id){
		if (id == 0)
			return blocks[id];
		else
			return blocks[id] - blocks[id -1];
	}

	
	
	/**
	 * A method to encode a block of data in a byte buffer
	 * @param n Hashmap of nodes
	 * @param i Hashmap of edges, organized by to id
	 * @param o Hashmap of edges, organized by from id
	 * @return the data in a ByteBuffer
	 */
	public static ByteBuffer blockToByteBuffer(HashMap<Integer, Node> n, HashMap<Integer, ArrayList<Edge>> i, HashMap<Integer, ArrayList<Edge>> o){
		int innerEdgeCount = 0;
		for (ArrayList<Edge> ae: i.values())
				innerEdgeCount += ae.size();
		
		int outerEdgeCount = 0;
		for (ArrayList<Edge> ae: o.values()){
			outerEdgeCount += ae.size();
		}
		ByteBuffer b = ByteBuffer.allocate(1 + 4+ (4+4+8)*n.size() + 8 * innerEdgeCount + 8 * outerEdgeCount + 8);
		b.put(CONST.ENTIRE_BLOCK_DATA_MARKER);
		b.putInt(n.size());
		for (Node nn : n.values()){
			b.putInt(nn.id);
			b.putInt(nn.edges());
			b.putDouble(nn.getPR());
			
		}
		b.putInt(innerEdgeCount);
		for (ArrayList<Edge> ae: i.values()){
			for (Edge e: ae){
				b.putInt(e.from);
				b.putInt(e.to);
			}
		}
		b.putInt(outerEdgeCount);
		for (ArrayList<Edge> ae: o.values()){
			for (Edge e: ae){
				b.putInt(e.from);
				b.putInt(e.to);
			}
		}
		return b;
	}
	/**
	 * A method to gather block information from a bytebuffer
	 * @param b the enclosing bytebuffer
	 * @param n Hashmap to insert nodes into
	 * @param i Hashmap to hold edges, organized by to id
	 * @param o Hashmap to hold edges, organized by from id
	 * @return
	 */
	public static double fillBlockFromByteBuffer(ByteBuffer b, HashMap<Integer, Node> n, HashMap<Integer, ArrayList<Edge>> i, HashMap<Integer, ArrayList<Edge>> o){
		b.get();
		int nodeCount = b.getInt();
		double sink = 0.;
		for (int ind = 0; ind < nodeCount; ind++){
			Node nn = new Node(b.getInt());
			nn.setEdges(b.getInt());
			nn.setPR(b.getDouble());
			n.put(nn.id, nn);
			if (nn.edges() == 0){
				sink+=nn.getPR();
			}
		}
		//these are organized by edge.to. i is null in the final pass
		int iCount = b.getInt();
		for (int ind = 0; ind < iCount && i != null; ind++){
			Edge e = new Edge(b.getInt(), b.getInt());
			if (i.containsKey(e.to))
				i.get(e.to).add(e);
			else{
				ArrayList<Edge> ae = new ArrayList<Edge>();
				ae.add(e);
				i.put(e.to,  ae);
			}
		}
		//these are organized by edge.from. o is null in the final pass
		if (i == null)
			b.position(b.position() + iCount * 8);
		int oCount = b.getInt();
		if (o != null){
			for (int ind = 0; ind < oCount && o != null; ind++){
				Edge e = new Edge(b.getInt(), b.getInt());
				if (o.containsKey(e.from))
					o.get(e.from).add(e);
				else{
					ArrayList<Edge> ae = new ArrayList<Edge>();
					ae.add(e);
					o.put(e.from,  ae);
				}
			}
		}
		
		return sink;
	}
	/**
	 * A method to turn an incoming value into a byte arrat
	 * @param to the edge this value goes to
	 * @param n the node the value is leaving from
	 * @return holding data
	 */
	public static byte[] incomingValue(int to, Node n){
		ByteBuffer b = ByteBuffer.allocate(13);
		b.put(CONST.INCOMING_EDGE_MARKER);
		b.putInt(to);
		b.putDouble(n.prOnEdge());
		return b.array();
	}
	
	
	

	public static int baseValue(byte b) {
		return  b == 0 ? 0 : blocks[b - 1];
	}

	

	public static boolean isInFirstTwoIndex(int i, int id) {
		return (i == 0 && id < 2) || (i > 0 && id < blocks[i-1] + 2);
	}
	
	
	
	
	
}
