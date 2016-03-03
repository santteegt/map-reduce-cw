package uk.ac.bbk.ucl.cw.pagerank.simple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import uk.ac.bbk.ucl.cw.pagerank.common.PageRankNode;

public class MassDistributionMapper extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
	    

		// Random jump factor.
		private static float ALPHA = 0.15f;
		private float missingMass = 0.0f;
	    private int nodeCnt = 0;

	    @Override
	    public void setup(Context context) throws IOException {
	      Configuration conf = context.getConfiguration();

	      missingMass = conf.getFloat("MissingMass", 0.0f);
	      nodeCnt = conf.getInt("NodeCount", 0);
	    }

	    @Override
	    public void map(IntWritable nid, PageRankNode node, Context context)
	        throws IOException, InterruptedException {
	      float p = node.getPageRank();

	      float jump = (float) (Math.log(ALPHA) - Math.log(nodeCnt));
	      float link = (float) Math.log(1.0f - ALPHA)
	          + PageRankMetrics.sumLogProbs(p, (float) (Math.log(missingMass) - Math.log(nodeCnt)));

	      p = PageRankMetrics.sumLogProbs(jump, link);
	      node.setPageRank(p);

	      context.write(nid, node);
	    }
	  }


