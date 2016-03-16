package uk.ac.ucl.irdm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Bigram counts
 * @author santteegt
 *
 */
public class BigramCount extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(BigramCount.class);

	/**
	 * Creates an instance of this tool.
	 */
	public BigramCount() {
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

		sLogger.info("Tool: BigramCount");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		Job job = Job.getInstance(getConf(), BigramCount.class.getSimpleName());

        Configuration conf = job.getConfiguration();
        job.setJobName(BigramCount.class.getSimpleName());
        job.setJarByClass(getClass());

        conf.setInt("mapred.map.tasks", mapTasks);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		//FileOutputFormat.setCompressOutput(conf, false);

		job.setOutputKeyClass(Bigram.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(BigramMapper.class);
		job.setCombinerClass(BigramReducer.class);
		job.setReducerClass(BigramReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(outputDir.toUri(), conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new BigramCount(), args);
		System.exit(res);
	}
}
