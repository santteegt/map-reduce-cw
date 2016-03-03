package uk.ac.bbk.ucl.cw.pagerank.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * 
 * @author santteegt
 *
 */
public class PageRankRecordMapper extends Mapper<LongWritable, Text, IntWritable, PageRankNode> {
	
	private static final Logger LOG = Logger.getLogger(PageRankRecordMapper.class);
	
    private static final IntWritable NID = new IntWritable();
    private static final PageRankNode NODE = new PageRankNode();
    
    private String headNode = null;
    private List<Integer> childNodes = new ArrayList<Integer>();

    @Override
    public void setup(Mapper<LongWritable, Text, IntWritable, PageRankNode>.Context context) {
      int n = context.getConfiguration().getInt("node.cnt", 0);
      NODE.setType(PageRankNode.Type.Complete);
      NODE.setPageRank((float) -StrictMath.log(n));
    }

    @Override
    public void map(LongWritable key, Text t, Context context) throws IOException,
        InterruptedException {
    	if(t.toString().startsWith("#")) return;
    	String[] arr = t.toString().split("\\s+");
    	if(headNode == null) {
    		headNode = arr[0];
    		childNodes.clear();
    	} 
    	if(arr[0].equals(headNode)) {
    		childNodes.add(Integer.valueOf(arr[1]));
    	} else {
    		NID.set(Integer.parseInt(headNode));
    		NODE.setNodeId(Integer.valueOf(headNode));
    		int[] neighbors = new int[childNodes.size()];
    		for (int i = 0; i < childNodes.size(); i++) {
    			neighbors[i] = childNodes.get(i).intValue();
    		}

    		NODE.setAdjacencyList(new ArrayListOfIntsWritable(neighbors));
    		context.getCounter("graph", "numNodes").increment(1);
        	context.getCounter("graph", "numEdges").increment(neighbors.length);
    		context.getCounter("graph", "numActiveNodes").increment(1);

        	context.write(NID, NODE);
        	
    		headNode = arr[0];
    		childNodes.clear();
    		childNodes.add(Integer.valueOf(arr[1]));
    		
    	}
    }

}
