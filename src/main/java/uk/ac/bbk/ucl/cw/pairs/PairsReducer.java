package uk.ac.bbk.ucl.cw.pairs;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.bbk.ucl.cw.util.Bigram;

import java.io.IOException;
import java.util.Iterator;

/**
 * 
 * @author santteegt
 *
 */
public class PairsReducer extends Reducer<Bigram, FloatWritable, Bigram, FloatWritable> {
	
	    private static final FloatWritable VALUE = new FloatWritable();
	    
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
	        VALUE.set(sum);
	        context.write(key, VALUE);
	        relativeCounts = sum;
	      } else {
	        VALUE.set(sum / relativeCounts);
	        context.write(key, VALUE);
	      }
	    }
}
