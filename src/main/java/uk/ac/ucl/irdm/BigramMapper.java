package uk.ac.ucl.irdm;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for Bigram counts
 * @author santteegt
 *
 */
public class BigramMapper  extends Mapper<LongWritable, Text, Bigram, LongWritable> {
	
	private String word = null;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = ((Text) value).toString();
		StringTokenizer itr = new StringTokenizer(line);

		String neighbor = null;
		while (itr.hasMoreTokens()) {
			this.word = itr.nextToken();
			if (neighbor != null) {
				Bigram bigramBean = new Bigram(word, neighbor); //stores the bigram representation
		        context.write(bigramBean, new LongWritable(1));
			}
			neighbor = this.word;
		}
	}

}