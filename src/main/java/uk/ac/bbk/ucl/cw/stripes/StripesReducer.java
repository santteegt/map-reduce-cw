package uk.ac.bbk.ucl.cw.stripes;

import org.apache.hadoop.mapreduce.Reducer;

import uk.ac.bbk.ucl.cw.util.AssociativeArray;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author santteegt
 *
 */
public class StripesReducer extends Reducer<Text, AssociativeArray, Text, AssociativeArray> {

    @Override
    protected void reduce(Text key, Iterable<AssociativeArray> values, Reducer<Text, AssociativeArray, Text, AssociativeArray>.Context context) throws IOException, InterruptedException {
        Iterator<AssociativeArray> iter = values.iterator();	
        AssociativeArray map = new AssociativeArray();

        while (iter.hasNext()) {
                map.getRelativeFrequency(iter.next());
        }

        context.write(key, map);
    }
}
