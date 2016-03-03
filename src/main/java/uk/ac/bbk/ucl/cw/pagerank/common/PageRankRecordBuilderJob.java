package uk.ac.bbk.ucl.cw.pagerank.common;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Build PageRank Records
 * @author santteegt
 *
 */
public class PageRankRecordBuilderJob extends Configured implements Tool {

  private static final String NODE_CNT_FIELD = "node.cnt";

  public int run(String[] args) throws Exception {
	  
	  Job job = Job.getInstance(getConf(), "BBKCCPageRankBuilder");
	
	  Configuration conf = job.getConfiguration();
	  job.setJarByClass(getClass());
	  
	  Path in = new Path( ((args.length >0 && args[0] != null) ?args[0]:"/home/malgia/NetBeansProjects/bbkccvbwordcount/input"));
	  Path out = new Path( ((args.length >1 && args[1] != null) ?args[1]:"/home/malgia/NetBeansProjects/bbkccvbwordcount/output/stripes"));
	  int n = (int) ((args.length >2 && args[2] != null) ? Integer.valueOf(args[2]): 5);
	  int numberReducers = (int) ((args.length >3 && args[3] != null) ? Integer.valueOf(args[3]): 0);
	  out.getFileSystem(conf).delete(out,true);

    
	  conf.setInt(NODE_CNT_FIELD, n);
	  conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

	  job.setNumReduceTasks(numberReducers);

	  FileInputFormat.addInputPath(job, in);
	  FileOutputFormat.setOutputPath(job, out);

	  job.setInputFormatClass(TextInputFormat.class);
	  job.setOutputFormatClass(SequenceFileOutputFormat.class);

	  job.setMapOutputKeyClass(IntWritable.class);
	  job.setMapOutputValueClass(PageRankNode.class);

	  job.setOutputKeyClass(IntWritable.class);
	  job.setOutputValueClass(PageRankNode.class);

	  job.setMapperClass(PageRankRecordMapper.class);

	  long timer = System.currentTimeMillis();
	  job.waitForCompletion(true);
	  System.out.println(String.format("****PAGE RANK RECORD BUILDER FINISHED: %.2f seconds", 
    		(System.currentTimeMillis() - timer) / Float.valueOf(1000) ));


	  return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PageRankRecordBuilderJob(), args);
  }
}
