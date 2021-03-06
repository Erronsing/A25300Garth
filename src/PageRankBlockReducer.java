import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Implements the subsequent Jobs reduce functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class PageRankBlockReducer extends
		Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {


		/** Overrites reduce
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(LongWritable key, Iterable<BytesWritable> vals, Context context){

			// Sets up hashmaps for vals
			HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
			HashMap<Integer, ArrayList<Edge>> innerEdges = new HashMap<Integer, ArrayList<Edge>>();
			HashMap<Integer, ArrayList<Edge>> outerEdges = new HashMap<Integer, ArrayList<Edge>>();
			HashMap<Integer, Double> incomingEdges = new HashMap<Integer, Double>();


			
			//we want to know the sum of the incoming edges, so well grab this now
			double totalIncoming = 0.;
			// Get values passed into function
			for (BytesWritable val : vals){
				byte marker = val.getBytes()[0];
				
				// Block Data
				if (marker == CONST.ENTIRE_BLOCK_DATA_MARKER){
					Util.fillBlockFromByteBuffer(ByteBuffer.wrap(val.getBytes()), nodes, innerEdges, outerEdges);
				} 
				// Incoming Edges
				else if (marker == CONST.INCOMING_EDGE_MARKER){
					ByteBuffer b = ByteBuffer.wrap(val.getBytes());
					b.get();
					int to = b.getInt();
					double pr = b.getDouble();
					totalIncoming += pr;
					if (incomingEdges.containsKey(to)){ // Check if we already know about this edge and add PR incoming to this node
						incomingEdges.put(to, incomingEdges.get(to) + pr);	
					} else {
						incomingEdges.put(to, pr);
					}

				}
			}


			// Get Data from counters for calculations, for the first round well need the basic sink
			double sinkPerNode = context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue()/CONST.SIG_FIG_FOR_TINY_DOUBLE_TO_LONG/CONST.TOTAL_NODES;
			//well increment the inner block rounds every round. We could do it all at once, but this doesn't create much overhead
			Counter innerBlockRounds = context.getCounter(PageRankEnum.INNER_BLOCK_ROUNDS);
			// Set up Maps for each pass of loop below
			HashMap<Integer, Node> nodesLastPass = new HashMap<Integer, Node>();
			HashMap<Integer, Node> nodesThisPass = new HashMap<Integer, Node>();
			
			double residualSum = Double.MAX_VALUE;
			
			// Each node is put into nodesLastPass for first pass
			for (Node n : nodes.values()){
				nodesLastPass.put(n.id, new Node(n));
			}
			// Run until converged in block

			
			double expectedSum = 0; // the sum we want to get out will go here
			double inBlockConstant = 1.; //we'll use this to get our expected sum
			double nodesInBlock = nodes.size();
			double beta = .25;
			double betasum = 0.;
			while (residualSum/nodesInBlock > CONST.RESIDUAL_SUM_DELTA/10.){
				residualSum = 0.;
				double newInBlockSink = 0; //in block sink
				double sumInPr = 0.; //total PR
				double newRedistSum = 0.; //outgoing PR
				for (Node n : nodesLastPass.values()){
					// Base PR
					double pr = CONST.RANDOM_SURFER * CONST.BASE_PAGE_RANK + CONST.DAMPING_FACTOR * inBlockConstant * sinkPerNode;
					
					// Incoming PR added in
					if (incomingEdges.containsKey(n.id))
						pr +=  CONST.DAMPING_FACTOR * inBlockConstant * incomingEdges.get(n.id);
					// In Block PR added in
					if (innerEdges.containsKey(n.id)){
						ArrayList<Edge> ae = innerEdges.get(n.id);
						for (Edge e : ae){
							Node nn = nodesLastPass.get(e.from);
							pr += CONST.DAMPING_FACTOR * inBlockConstant * nn.prOnEdge();
						}
					}
					pr = betasum*n.getPR() + (1-betasum)*pr;
					//calculate PR we'll lose from this node next round
					if (outerEdges.containsKey(n.id)){
						newRedistSum += pr/(double)n.edges() * outerEdges.get(n.id).size() ; 
					} else if (n.edges() == 0){
						newInBlockSink += pr;
					}
					// Calculate Residual
					double residual = Math.abs((pr - n.getPR()))/pr;
					residualSum += residual;
					// Add node to nodesThisPass since we have processed it
					Node nPrime = new Node(n);
					nPrime.setPR(pr);
					nodesThisPass.put(nPrime.id, nPrime);
					sumInPr += pr;
				}
				//save the exp. sum if this is the first pass
				if (expectedSum == 0)
					expectedSum = sumInPr;
				betasum += (1-betasum)*beta;
				// Reset Holders for next round.
				// nodesThisPass becomes lastPass
				// Reset NodesThisPass
				nodesLastPass = nodesThisPass;
				nodesThisPass = new HashMap<Integer, Node>();
				//expected value / value we will get
				inBlockConstant = (expectedSum 
									- CONST.BASE_PAGE_RANK * CONST.RANDOM_SURFER * nodesInBlock )
									/ (CONST.DAMPING_FACTOR * (sumInPr - newRedistSum - newInBlockSink + totalIncoming + sinkPerNode *  nodesInBlock ));
				
				// Check if we have converged
				//System.out.println(key + " " + residualSum);
				innerBlockRounds.increment(1);
			}
			
			// Once we converge Calculate block data
			double residualSumOuter = 0.;
			for (Node n : nodesLastPass.values()){
				double residual = Math.abs((n.getPR() - nodes.get(n.id).getPR()))/n.getPR();
				residualSumOuter += residual;
			}
			context.getCounter(PageRankEnum.RESIDUAL_SUM).increment((long) (residualSumOuter * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG));
			// Save updated Block data
			ByteBuffer block = Util.blockToByteBuffer(nodesLastPass, innerEdges, outerEdges);
			try {
				context.write(key, new BytesWritable(block.array()));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	

}
