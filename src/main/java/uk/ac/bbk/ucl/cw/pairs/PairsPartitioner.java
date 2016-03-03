package uk.ac.bbk.ucl.cw.pairs;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import uk.ac.bbk.ucl.cw.util.Bigram;

public class PairsPartitioner extends Partitioner<Bigram, FloatWritable> {
	
    @Override
    public int getPartition(Bigram key, FloatWritable value, int numReduceTasks) {
		return (key.getNeighbor().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
    
}
