package uk.ac.bbk.ucl.cw.pairs;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import uk.ac.bbk.ucl.cw.util.Bigram;

import java.io.IOException;
import java.io.StringReader;

/**
 * 
 * @author santteegt
 *
 */
public class PairsMapper extends Mapper<LongWritable, Text, Bigram, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final Bigram BIGRAM = new Bigram();

    @Override
    protected void map(LongWritable key, Text line, Mapper<LongWritable, Text, Bigram, FloatWritable>.Context context) 
    		throws IOException, InterruptedException {

      StringReader reader = new StringReader(line.toString());
      PTBTokenizer<CoreLabel> ptbt = new PTBTokenizer<>(reader, 
    		  new CoreLabelTokenFactory(), "");
		
      String rightNeighbor = null;
      while (ptbt.hasNext()) {
		  CoreLabel label = ptbt.next();
		  if (rightNeighbor != null) {
	          BIGRAM.setNeighbor(rightNeighbor);
	          BIGRAM.setWord(label.toString());
	          context.write(BIGRAM, ONE);
	
	          BIGRAM.setNeighbor(rightNeighbor);
	          BIGRAM.setWord("*"); //neighbor frequency counts 
	          context.write(BIGRAM, ONE);
		  }
		  rightNeighbor = label.toString();
      }
    }
  }