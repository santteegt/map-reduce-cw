package uk.ac.bbk.ucl.cw.pagerank.simple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import tl.lin.data.array.ArrayListOfIntsWritable;
import uk.ac.bbk.ucl.cw.pagerank.common.PageRankNode;

public class PageRankMapper extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    private static final IntWritable neighbor = new IntWritable();

    private static final PageRankNode intermediateMass = new PageRankNode();

    private static final PageRankNode intermediateStructure = new PageRankNode();

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;

      if (node.getAdjacenyList().size() > 0) {
        ArrayListOfIntsWritable list = node.getAdjacenyList();
        float mass = node.getPageRank() - (float) StrictMath.log(list.size());

        context.getCounter(PageRank.edges).increment(list.size());
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(PageRankNode.Type.Mass);
          intermediateMass.setPageRank(mass);
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }
