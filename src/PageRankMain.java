
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
/** 
 * Main class for running Node PageRank
 * @author Alice, Spencer, Garth
 *
 */
public class PageRankMain {
	/**
	 * Main method for running PageRank
	 * @param args input output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    
	    // Set up job 0
	    String outputFile = args[1] + " pass 0";
	    
	    Job job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");
	    job.setJarByClass(PageRankMain.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    job.setMapperClass(PageRankMapperZero.class);
	    job.setReducerClass(PageRankReducerZero.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);    
        job.waitForCompletion(true);
        int round = 1;
        // Run for 5 rounds as specified by assignment
        while (round < 6){
        	String inputFile = outputFile;
        	outputFile = args[1] + " pass " + round;
        	conf = new Configuration();
        	job = Job.getInstance(conf, "page rank " + args[1] + " pass 0");

        	FileInputFormat.setInputPaths(job, new Path(inputFile));
     	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
     	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
     	    job.setInputFormatClass(SequenceFileInputFormat.class);
     	    job.setJarByClass(PageRankMain.class);
     	    job.setMapperClass(PageRankMapper.class);
     	    job.setReducerClass(PageRankReducer.class);
     	    job.setOutputKeyClass(LongWritable.class);
     	    job.setOutputValueClass(Text.class);
            job.setJarByClass(PageRankMain.class);
            
            job.waitForCompletion(true);
            // Calculate residual from counters
            double residualSum = job.getCounters().findCounter(PageRankEnum.RESIDUAL_SUM).getValue()/CONST.SIG_FIG_FOR_DOUBLE_TO_LONG;

            // Output Job data
            System.out.println("Round: " + round + 
        			"\nResidual sum (across all nodes): " + residualSum + " avg: " + residualSum/CONST.TOTAL_NODES + "\n");
            
            round++;
        	
        }
	   
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	}

        
}
