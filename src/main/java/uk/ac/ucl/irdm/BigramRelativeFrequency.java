package uk.ac.ucl.irdm;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * BIGRAM Relative Frequency Count
 * @author santteegt
 *
 */
public class BigramRelativeFrequency extends Configured implements Tool {
	
	private static final Logger sLogger = Logger.getLogger(BigramRelativeFrequency.class);
	
	/**
	 * Mapper class
	 * @author santteegt
	 *
	 */
	protected static class PairsMapper extends Mapper<LongWritable, Text, Bigram, FloatWritable> {

	    private static final Bigram BIGRAM = new Bigram();

	    @Override
	    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Bigram, FloatWritable>.Context context) 
	    		throws IOException, InterruptedException {
	    	
	    	String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);

			FloatWritable one = new FloatWritable(1);
			String neighbor = null;
			while (itr.hasMoreTokens()) {

			  String word = itr.nextToken();
			  if (neighbor != null) {
		          BIGRAM.setNeighbor(neighbor);
		          BIGRAM.setWord(word);
		          context.write(BIGRAM, one);
		
		          BIGRAM.setNeighbor(neighbor);
		          BIGRAM.setWord("*"); //neighbor frequency counts 
		          context.write(BIGRAM, one);
			  }
			  neighbor = word;
	      }
	    }
	  }
	
	/**
	 * Combiner that counts bigram pairs
	 * @author santteegt
	 *
	 */
	protected static class SumCombiner extends Reducer<Bigram, FloatWritable, Bigram, FloatWritable> {

	  @Override
	  public void reduce(Bigram key, Iterable<FloatWritable> values, Context context) 
	      throws IOException, InterruptedException {
	    float sum = 0;
	    Iterator<FloatWritable> iter = values.iterator();
	    while (iter.hasNext()) {
	      sum += iter.next().get();
	    }
	    context.write(key, new FloatWritable(sum));
	  }
	}
	
	/**
	 * Partitioner to send all the bigrams with the same left word to the same reducer
	 * @author santteegt
	 *
	 */
	protected static class PairsPartitioner extends Partitioner<Bigram, FloatWritable> {
		
	    @Override
	    public int getPartition(Bigram key, FloatWritable value, int numReduceTasks) {
			return (key.getNeighbor().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	    }
	    
	}
	
	/**
	 * Reducer class
	 * @author santteegt
	 *
	 */
	protected static class PairsReducer extends Reducer<Bigram, FloatWritable, Bigram, FloatWritable> {
	    
	    private float relativeCounts = 0.0f;
	    

	    @Override
	    public void reduce(Bigram key, Iterable<FloatWritable> values, Context context)
	        throws IOException, InterruptedException {
	      float sum = 0.0f;
	      Iterator<FloatWritable> iter = values.iterator();
	      while (iter.hasNext()) {
	        sum += iter.next().get();
	      }

	      if (key.getWord().equals("*")) {
	        context.write(key, new FloatWritable(sum));
	        relativeCounts = sum;
	      } else {
	        context.write(key, new FloatWritable(sum / relativeCounts));
	      }
	    }
	}


    /**
	 * Creates an instance of this tool.
	 */
	public BigramRelativeFrequency() {
		
	}

	/**
	 *  Prints argument options
	 * @return
	 */
	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool: BigramRalativeFrequency");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		Job job = Job.getInstance(getConf(), BigramRelativeFrequency.class.getSimpleName());

        Configuration conf = job.getConfiguration();
        job.setJobName(BigramRelativeFrequency.class.getSimpleName());
        job.setJarByClass(getClass());
        
        //conf.setInt("mapred.map.tasks", mapTasks);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        job.setMapperClass(PairsMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(PairsReducer.class);
        job.setPartitionerClass(PairsPartitioner.class);
        
        
        job.setMapOutputKeyClass(Bigram.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Bigram.class);
        job.setOutputValueClass(FloatWritable.class);
        
        // Delete the output directory if it exists already
 		Path outputDir = new Path(outputPath);
 		FileSystem.get(outputDir.toUri(), conf).delete(outputDir, true);
        
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
        return 0;
    }
	
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new BigramRelativeFrequency(), args);
        System.exit(result);
    }
}
