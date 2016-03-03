package uk.ac.bbk.ucl.cw.common;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.bbk.ucl.cw.util.Bigram;

/**
 * Computes the sum of all bigram pairs
 * @author santteegt
 *
 */
public class SumCombiner extends 
Reducer<Bigram, FloatWritable, Bigram, FloatWritable> {

  private static final FloatWritable SUM = new FloatWritable();

  @Override
  public void reduce(Bigram key, Iterable<FloatWritable> values, Context context) 
      throws IOException, InterruptedException {
    int sum = 0;
    Iterator<FloatWritable> iter = values.iterator();
    while (iter.hasNext()) {
      sum += iter.next().get();
    }
    SUM.set(sum);
    context.write(key, SUM);
  }
}
