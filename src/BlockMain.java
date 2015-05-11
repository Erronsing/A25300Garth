
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/** 
 * Main class for running Blocked PageRank
 * @author Alice, Spencer, Garth
 *
 */
public class BlockMain {
	/**
	 * Runs the Blocked PageRank map reduce jobs
	 * @param args input and output location (for Amazon EMR these should be in s3)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration(); 
	    
	    // Creating the Job for the first pass - processing input file
	    String outputFile = args[1] + " pass 0";
	    
	    Job job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
	    
	    job.setJarByClass(BlockMain.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    
	    job.setMapperClass(BlockMapperPass0.class);
	    job.setReducerClass(BlockReducerPass0.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(BytesWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);     
        job.waitForCompletion(true);
        int round = 1;
        double residualSum = Double.MAX_VALUE;
        String inputFile;
        
        // After first pass we loop through several jobs until the termination condition is met
        // Termination Condition: resdidual sum < .001
        while (residualSum/CONST.TOTAL_NODES > CONST.RESIDUAL_SUM_DELTA){
        	// Last runs output is input for the next run
        	inputFile = outputFile; 
        	
        	// Set up next job config
        	outputFile = args[1] + " pass " + round;
        	conf = new Configuration(); 
        	job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
        	FileInputFormat.setInputPaths(job, new Path(inputFile));
     	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    	    job.setJarByClass(BlockMain.class);
    	    
     	    job.setMapperClass(PageRankBlockMapper.class);
     	    job.setReducerClass(PageRankBlockReducer.class);
     	    job.setOutputKeyClass(LongWritable.class);
     	    job.setOutputValueClass(BytesWritable.class);
             
     	    job.setInputFormatClass(SequenceFileInputFormat.class);
     	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
             
            job.waitForCompletion(true);
            
            // After job completes - add up necessary counters and output info
			org.apache.hadoop.mapreduce.Counter innerBlockRounds = job.getCounters().findCounter(PageRankEnum.INNER_BLOCK_ROUNDS);
			
            residualSum = job.getCounters().findCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;
        	System.out.println("Round: " + round + 
        			" \nInner block rounds total: " + innerBlockRounds.getValue() + " avg " + innerBlockRounds.getValue()/68. +
        			"\nResidual sum (across all nodes): " + residualSum + " avg: " + residualSum/CONST.TOTAL_NODES + "\n");
        	
            round++;
        	
        }
        
        // After completing rounds above we do one more job to get specific data for write up
        inputFile = outputFile;
    	outputFile = args[1] + " pageRank output.txt";
    	conf = new Configuration();
    	job = Job.getInstance(conf, "page rank " + args[1] + " pass get final nodes");
    	FileInputFormat.setInputPaths(job, new Path(inputFile));
 	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    job.setJarByClass(BlockMain.class);
	    
 	    job.setMapperClass(GetFinalNodesMapper.class);
 	    job.setReducerClass(Reducer.class);
 	    job.setOutputKeyClass(LongWritable.class);
 	    job.setOutputValueClass(Text.class);
         
 	    job.setInputFormatClass(SequenceFileInputFormat.class);
 	    job.setOutputFormatClass(TextOutputFormat.class);
         
        job.waitForCompletion(true);
		
        System.exit(0);
        
	}

        
}
