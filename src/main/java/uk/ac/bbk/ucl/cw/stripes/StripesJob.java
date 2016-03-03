package uk.ac.bbk.ucl.cw.stripes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.bbk.ucl.cw.util.AssociativeArray;

/**
 * BIGRAM Stripes
 * @author santteegt
 *
 */
public class StripesJob extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new StripesJob(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "BBKCCStripes");

        Configuration conf = job.getConfiguration();
        job.setJarByClass(getClass());
        
        Path in = new Path( ((args.length >0 && args[0] != null) ?args[0]:"/home/malgia/NetBeansProjects/bbkccvbwordcount/input"));
        Path out = new Path( ((args.length >1 && args[1] != null) ?args[1]:"/home/malgia/NetBeansProjects/bbkccvbwordcount/output/stripes"));
        int reduceTasks = (int) ((args.length >2 && args[2] != null) ? args[2]: 5);
        out.getFileSystem(conf).delete(out,true);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJarByClass(StripesJob.class);
        job.setNumReduceTasks(reduceTasks);
        
        job.setMapperClass(StripesMapper.class);
        job.setCombinerClass(StripesReducer.class);
        job.setReducerClass(StripesReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AssociativeArray.class);

        long timer = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println(String.format("****BIGRAM BY STRIPES METHOD FINISHED: %.2f seconds", 
        		(System.currentTimeMillis() - timer) / Float.valueOf(1000) ));


        return 0;
    }
}
