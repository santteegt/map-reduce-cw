package uk.ac.bbk.ucl.cw.pairs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.bbk.ucl.cw.common.SumCombiner;
import uk.ac.bbk.ucl.cw.util.Bigram;

/**
 * BIGRAM by Pairs Method
 * @author santteegt
 *
 */
public class PairsJob extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new PairsJob(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "BBKCCPairs");

        Configuration conf = job.getConfiguration();
        job.setJobName(PairsJob.class.getSimpleName());
        job.setJarByClass(getClass());
        
        Path in = new Path( ((args.length >0 && args[0] != null) ?args[0]:"/home/malgia/NetBeansProjects/bbkccvbwordcount/input"));
        Path out = new Path( ((args.length >1 && args[1] != null) ?args[1]:"/home/malgia/NetBeansProjects/bbkccvbwordcount/output/pairs"));
        //int mapTasks = (int) ((args.length >2 && args[2] != null) ? Integer.valueOf((String)args[2]): 5);
        int reduceTasks = (int) ((args.length >2 && args[2] != null) ? Integer.valueOf((String)args[2]): 5);
        out.getFileSystem(conf).delete(out,true);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJarByClass(PairsJob.class);
        
        //conf.setInt("mapred.tasktracker.map.tasks.maximum", mapTasks);
        //conf.setInt("mapred.map.tasks", mapTasks);
        job.setNumReduceTasks(reduceTasks);
        
        job.setMapperClass(PairsMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(PairsReducer.class);
        job.setPartitionerClass(PairsPartitioner.class);
        
        job.setMapOutputKeyClass(Bigram.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Bigram.class);
        job.setOutputValueClass(FloatWritable.class);
        
        long timer = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println(String.format("****BIGRAM BY PAIRS METHOD FINISHED: %.2f seconds", 
        		(System.currentTimeMillis() - timer) / Float.valueOf(1000) ));


        return 0;
    }
}
