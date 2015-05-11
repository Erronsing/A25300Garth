import org.apache.hadoop.io.Text;


/**
 * Basic edge object
 * @author Alice, Spencer, Garth
 *
 */
public class Edge {

	public final int from;
	public final int to;
	public final boolean inBlock;
	
	/**
	 * Creates basic edge object from int
	 * @param from nodeid
	 * @param to nodeidS
	 */
	public Edge(int from, int to){
		this.from = from;
		this.to = to;
		this.inBlock = Util.idToBlock(from) == Util.idToBlock(to);
	}
	/** 
	 * Creates edge from int values representing from to
	 * @param from nodeid
	 * @param to nodeid
	 * @param inBlock is in block
	 */
	public Edge(int from, int to, boolean inBlock){
		this.from = from;
		this.to = to;
		this.inBlock = inBlock;
	}
	/**
	 * Creates Edge from two node objects
	 * @param from node
	 * @param to node
	 */
	public Edge(Node from, Node to){
		this.from = from.id;
		this.to = to.id;
		inBlock = true;
		from.addBranch();
	}
	/**
	 * Creates Edge based on string to -> from
	 * @param edge to -> from
	 * @param inBlock specifies if this is an internal edge or external
	 */
	public Edge(String edge, boolean inBlock){
		String[] info = edge.split(CONST.L2_DIV);
		this.from = Integer.parseInt(info[CONST.EDGE_FROM]);
		this.to = Integer.parseInt(info[CONST.EDGE_TO]);
		this.inBlock = inBlock;
	}
	/**
	 * Creates an Edge 
	 * @param from node
	 * @param to node
	 */
	public Edge(Node from, int to){
		this.from = from.id;
		this.to = to;
		inBlock = false;
		from.addBranch();
		
	}
	/** String Representation of Edgelang.Object#toString()
	 */
	public String toString(){
		return from + CONST.L2_DIV + to;
	}
	/**
	 * Gets the Haddop text object
	 * @return Hadoop Text Object
	 */
	public Text toText(){
		return new Text(toString());
	}
	public boolean equals (Edge e){
		return e.to == to && e.from == from;
		
	}
}
