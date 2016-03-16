package uk.ac.ucl.irdm;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

/**
 * Inverted index
 * @author santteegt
 *
 */
public class BuildInvertedIndex extends Configured implements Tool {
	
	private static final Logger sLogger = Logger.getLogger(BuildInvertedIndex.class);
	
	/**
	 * Mapper class
	 * @author santteegt
	 *
	 */
	private static class IndexMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
	
		@Override
		public void map(LongWritable docno, Text doc, Context context)
		    throws IOException, InterruptedException {
		  Object2IntFrequencyDistribution<String> wordFreq = new Object2IntFrequencyDistributionEntry<String>();
		  String text = doc.toString();
		  String[] terms = text.split("\\s+");
		
		  for (String term : terms) {
		    if (term != null && term.length() > 0) wordFreq.increment(term);
		  }
		
		  for (PairOfObjectInt<String> e : wordFreq) {
		    context.write(new Text(e.getLeftElement()), new PairOfInts((int) docno.get(), e.getRightElement()));
		  }
		}
	  }

	/**
	 * Reducer class
	 * @author santteegt
	 *
	 */
	private static class IndexReducer extends
      Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {

		@Override
		public void reduce(Text key, Iterable<PairOfInts> values, Context context)
		    throws IOException, InterruptedException {
		  Iterator<PairOfInts> iter = values.iterator();
		  ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
		
		  int df = 0;
		  while (iter.hasNext()) {
		    postings.add(iter.next().clone());
		    df++;
		  }
		
		  Collections.sort(postings);
		  context.write(key, new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(new IntWritable(df), postings));
		}
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

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);
		
		sLogger.info("Tool: BuildInvertedIndex");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);
		
		Job job = Job.getInstance(getConf(), BuildInvertedIndex.class.getSimpleName());

        Configuration conf = job.getConfiguration();
        job.setJobName(BuildInvertedIndex.class.getSimpleName());
        job.setJarByClass(getClass());
        
        conf.setInt("mapred.map.tasks", mapTasks);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfInts.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);
        
        job.setOutputFormatClass(MapFileOutputFormat.class);
        
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
        int result = ToolRunner.run(new BuildInvertedIndex(), args);
        System.exit(result);
    }
}
