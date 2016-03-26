package uk.ac.ucl.irdm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
	  
	if (args.length != 2) {
			System.out.println("usage: [input-path] [dataset-path]");
		System.exit(-1);
	}
	
	System.out.println("input path: " + args[0]);
	System.out.println("dataset path: " + args[1]);
	
	String indexPath = args[0];
	String datasetPath = args[1];

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    MapFile.Reader reader = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), config);

    searchTerm("king", reader, fs, datasetPath);
    searchTerm("macbeth", reader, fs, datasetPath);
    searchTerm("juliet", reader, fs, datasetPath);
    searchTerm("martino", reader, fs, datasetPath);

    reader.close();
  }

  /**
   * search a term frequencies using the inverted index.
   * @param term
   * @param reader
   * @param fs
   * @param datasetPath
   * @throws IOException
   */
  public static void searchTerm(String term, MapFile.Reader reader, FileSystem fs, String datasetPath) throws IOException {

	FSDataInputStream dataset = fs.open(new Path(datasetPath));
    Text key = new Text(term);
    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value = 
    		new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();
    Writable tuple = reader.get(key, value);

    if (tuple == null) {
      System.out.println(String.format("Term \"%s\" was not found on the corpus", term));
      return;
    }

    ArrayListWritable<PairOfInts> postingList = value.getRightElement();
    int df = value.getLeftElement().get();
    System.out.println(String.format("Postings list for \"%s\" with df = %d :", term, df));

    Int2IntFrequencyDistribution histogram = new Int2IntFrequencyDistributionEntry();
    for (PairOfInts pair : postingList) {
      histogram.increment(pair.getRightElement());
      dataset.seek(pair.getLeftElement());
      BufferedReader bReader = new BufferedReader(new InputStreamReader(dataset));
      
      String line = bReader.readLine();
      
      if(df == 1) {
    	  System.out.println( "DocNo(byte offset) of term: " + pair );
    	  System.out.println( "line: " + line.substring(0, line.length()>100?100:line.length()) );
      }
    }

    System.out.println(String.format("Histogram for \"%s\"", term));
    for (PairOfInts pair : histogram) {
      System.out.println(String.format("(%d, %d)", pair.getLeftElement(), pair.getRightElement()));
    }
  }
}
