package uk.ac.bbk.ucl.cw.stripes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.bbk.ucl.cw.util.AssociativeArray;

import java.io.IOException;

/**
 * 
 * @author santteegt
 *
 */
public class StripesMapper extends Mapper<LongWritable, Text, Text, AssociativeArray>{

	private static final int NGRAM_LENGTH = 2;
	private final Text word = new Text();
	private final AssociativeArray wordHashMap = new AssociativeArray();
    

    @Override
    public void map(LongWritable key, Text line, Mapper<LongWritable, Text, Text, AssociativeArray>.Context context) throws IOException, InterruptedException {
        String text = line.toString();

        String[] terms = text.split("\\s+");

        for (int i = 0; i < terms.length; i++) {
            String term = terms[i];
            if (term.length() != 0) {
                wordHashMap.clear();
                for (int j = i - NGRAM_LENGTH; j < i + NGRAM_LENGTH; j++) {
                    if ((j != i) && (j >= 0)) {
                        if (j >= terms.length) {
                                break;
                        }
                        if (terms[j].length() != 0) {
                            if (wordHashMap.containsKey(terms[j])) {
                            	wordHashMap.increment(terms[j]);
                            } else {
                            	wordHashMap.put(terms[j], 1);
                            }
                        }
                    }
                }
                word.set(term);
                if (wordHashMap.containsKey("*")) { //to compute relative frequencies
                	wordHashMap.increment("*");
                } else {
                	wordHashMap.put("*", 1);
                }
                context.write(word, wordHashMap);
            }
        }
    }
}