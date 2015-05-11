



/**
 * Simple Node Object
 * @author Alice, Spencer, Garth
 *
 */
public class Node {
	public final int id;
	private double pr = 0;
	private int edges = 0;
	/**
	 * Creates Node from int
	 * @param id nodeid
	 */
	public Node(int id){
		this.id = id;
	}
	/**
	 * Creates Node from other node
	 * @param n node
	 */
	public Node(Node n){
		this.id = n.id;
		this.edges = n.edges;
		this.pr = n.pr;
	}
	/**
	 * Creates Node from text
	 * @param nodeAsText tex reprentation of node
	 */
	public Node(String nodeAsText){
		String[] info = nodeAsText.split(CONST.L2_DIV);
		id = Integer.parseInt(info[0]);
		if (info.length > 1){
			pr = Double.parseDouble(info[1]);
		} 
	}

	/**
	 * Sets PageRank value
	 * @param set PR value
	 */
	public void setPR(double set){
		this.pr = set;
	}
	/**
	 * Adds an edge
	 */
	public void addBranch(){
		edges++;
	}
	/**
	 * Getter for PageRank
	 * @return pageRank value
	 */
	public double getPR(){
		return pr;
	}

	/**
	 * Returns edge PageRank value.
	 * i.e. the page rank divided over all edges
	 * @return pr value
	 */
	public double prOnEdge(){
		if (edges > 0)
			return pr/(double)edges;
		else return 0.;
	}
	/** 
	 * String representation of Node
	 * @see java.lang.Object#toString()
	 */
	public String toString(){
		return id + CONST.L2_DIV + pr;
	}

	/**
	 * Number of edges for node
	 * @return numEdges
	 */
	public int edges(){ return this.edges; }
	/**
	 * Debuger method for checking edge counts
	 * @return
	 */
	public String debug(){
		return "ID: " + id + " EDGES " + edges;
	}
	/**
	 * Set the edge count of the node
	 * @param int1
	 */
	public void setEdges(int int1) {
		edges = int1;
		
	}
	
}
