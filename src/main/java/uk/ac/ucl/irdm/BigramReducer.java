package uk.ac.ucl.irdm;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for Bigram counts
 * @author santteegt
 *
 */
public class BigramReducer extends Reducer<Bigram, LongWritable, Bigram, LongWritable> {

	    @Override
	    public void reduce(Bigram key, Iterable<LongWritable> values, Context context)
	        throws IOException, InterruptedException {
	    	long sum = 0;
	        Iterator<LongWritable> iter = values.iterator();
	        while (iter.hasNext()) {
	          sum += iter.next().get();
	        }
	        context.write(key, new LongWritable(sum));
	    }

}
