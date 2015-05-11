
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
 * Runs the Gauss Map Reduce
 * @author Alice, Spencer, Garth
 *
 */
public class GaussMain {
	/**
	 * @param args input output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    
	    // Set up first job to process input file notice it uses same mapper and reducer as block
	    String outputFile = args[1] + " pass 0";
	    
	    Job job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
	    
	    job.setJarByClass(GaussMain.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    
	    job.setMapperClass(BlockMapperPass0.class);
	    job.setReducerClass(BlockReducerPass0.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(BytesWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);     
        job.waitForCompletion(true);
        
        // Set up for looped jobs
        int round = 1;
        double residualSum = Double.MAX_VALUE;
        String inputFile;
        
        // Continue to loop until residual sum is low enough using Block Mapper and Gauss Reducer
        while (residualSum/CONST.TOTAL_NODES*10 > CONST.RESIDUAL_SUM_DELTA){
        
        	// Set up subsequent jobs
        	inputFile = outputFile;
        	outputFile = args[1] + " pass " + round;
        	conf = new Configuration();
        	job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
        	FileInputFormat.setInputPaths(job, new Path(inputFile));
     	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    	    job.setJarByClass(GaussMain.class);
    	    
     	    job.setMapperClass(PageRankBlockMapper.class);
     	    job.setReducerClass(GaussBlockReducer.class);
     	    job.setOutputKeyClass(LongWritable.class);
     	    job.setOutputValueClass(BytesWritable.class);
             
     	    job.setInputFormatClass(SequenceFileInputFormat.class);
     	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
             
            job.waitForCompletion(true);
            
            // Get counters
			org.apache.hadoop.mapreduce.Counter innerBlockRounds = job.getCounters().findCounter(PageRankEnum.INNER_BLOCK_ROUNDS);
            residualSum = job.getCounters().findCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;
            
            // Output information
        	System.out.println("Round: " + round + 
        			" \nInner block rounds total: " + innerBlockRounds.getValue() + " avg " + innerBlockRounds.getValue()/68. +
        			"\nResidual sum (across all nodes): " + residualSum + " avg: " + residualSum/CONST.TOTAL_NODES + "\n");
        	
            round++;
        	
        }
        
        // Create desired output files
        inputFile = outputFile;
    	outputFile = args[1] + " pageRank output.txt";
    	conf = new Configuration();
    	conf.setLong("TOTAL_NODES", 685230);
    	job = Job.getInstance(conf, "page rank " + args[1] + " pass get final nodes");
    	FileInputFormat.setInputPaths(job, new Path(inputFile));
 	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    job.setJarByClass(GaussMain.class);
	    
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
