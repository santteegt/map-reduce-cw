package uk.ac.bbk.ucl.cw.pagerank.simple;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.bbk.ucl.cw.pagerank.common.PageRankNode;
import uk.ac.bbk.ucl.cw.util.NonSplitableSequenceFileInputFormat;

public class PageRankJob extends Configured implements Tool {

	private static NumberFormat formatter = new DecimalFormat("0000");

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PageRankJob(), args);
	}

	public PageRankJob() {
		
	}
  
  public int run(String[] args) throws Exception {
	  Job job = Job.getInstance(getConf(), "BBKCCPageRank");
	  job.setJarByClass(getClass());
	  
	  String basePath = ((args.length >0 && args[0] != null) ?args[0]:"/home/malgia/NetBeansProjects/bbkccvbwordcount/input");
	  int n = (int) ((args.length >1 && args[1] != null) ? Integer.valueOf(args[1]): 5);
	  int numberIterations = (int) ((args.length >2 && args[2] != null) ? Integer.valueOf(args[2]): 0);

	  for (int i = 0; i < numberIterations; i++) {
		  iteratePageRank(i, i + 1, basePath, n);
	  }

	  long timer = System.currentTimeMillis();
	  job.waitForCompletion(true);
	  System.out.println(String.format("****PAGE RANK \"COMPLETED\" FINISHED: %.2f seconds", 
    		(System.currentTimeMillis() - timer) / Float.valueOf(1000) ));
	  return 0;
  }


  private void iteratePageRank(int i, int j, String basePath, int numNodes) throws Exception {
    
    float mass = getPageRank(i, j, basePath, numNodes);
    float missing = 1.0f - (float) StrictMath.exp(mass);
    massDistribution(i, j, missing, basePath, numNodes);
  }

  private float getPageRank(int i, int j, String basePath, int numNodes) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("GetPageRank Iteration" + j);
    job.setJarByClass(PageRankJob.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outMass = out + "-mass";

    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }
    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().set("PageRankMassPath", outMass);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(PageRankMapper.class);
    job.setReducerClass(PageRankReducer.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outMass), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Page Rank 1st stage finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    float mass = Float.NEGATIVE_INFINITY;
    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outMass))) {
      FSDataInputStream fin = fs.open(f.getPath());
      mass = PageRankMetrics.sumLogProbs(mass, fin.readFloat());
      fin.close();
    }

    return mass;
  }

  private void massDistribution(int i, int j, float missing, String basePath, int numNodes) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("Mass Distribution Job" + j);
    job.setJarByClass(PageRankJob.class);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().setFloat("MissingMass", (float) missing);
    job.getConfiguration().setInt("NodeCount", numNodes);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MassDistributionMapper.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long timer = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Mass Distribution Job Finished in in " + (System.currentTimeMillis() - timer) / 1000.0 + " seconds");
  }

}
