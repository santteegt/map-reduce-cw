package uk.ac.ucl.irdm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Int2IntFrequencyDistribution;
import tl.lin.data.fd.Int2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

/**
 * Lookup postings on an inverted index with MapFile format
 * @author santteegt
 *
 */
public class LookupPostings {

  public static void main(String[] args) throws IOException {
	  
	if (args.length != 1) {
			System.out.println("usage: [input-path]");
		System.exit(-1);
	}
	
	System.out.println("input path: " + args[0]);
	
	String indexPath = args[0];

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    MapFile.Reader reader = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), config);

    searchTerm("king", reader, fs);
    searchTerm("macbeth", reader, fs);
    searchTerm("juliet", reader, fs);
    searchTerm("martino", reader, fs);

    reader.close();
  }

  public static void searchTerm(String term, MapFile.Reader reader, FileSystem fs) throws IOException {

    Text key = new Text(term);
    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value = 
    		new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();
    Writable tuple = reader.get(key, value);

    if (tuple == null) {
      System.out.println(String.format("Term \"%s\" was not found on the corpus", term));
      return;
    }

    ArrayListWritable<PairOfInts> postingList = value.getRightElement();
    System.out.println(String.format("Postings list for \"%s\" with df = %d :", term, value.getLeftElement().get()));

    Int2IntFrequencyDistribution histogram = new Int2IntFrequencyDistributionEntry();
    for (PairOfInts pair : postingList) {
      histogram.increment(pair.getRightElement());
    }

    System.out.println(String.format("Histogram for \"%s\"", term));
    for (PairOfInts pair : histogram) {
      System.out.println(String.format("(%d, %d)", pair.getLeftElement(), pair.getRightElement()));
    }
  }
}
