package uk.ac.bbk.ucl.cw.pagerank.simple;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Preconditions;

import tl.lin.data.array.ArrayListOfIntsWritable;
import uk.ac.bbk.ucl.cw.pagerank.common.PageRankNode;

public class PageRankReducer extends Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    private float totalMass = Float.NEGATIVE_INFINITY;

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
    	
      Iterator<PageRankNode> values = iterable.iterator();
      PageRankNode node = new PageRankNode();

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      float mass = Float.NEGATIVE_INFINITY;
      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          ArrayListOfIntsWritable list = n.getAdjacenyList();
          structureReceived++;
          node.setAdjacencyList(list);
        } else {
          mass = PageRankMetrics.sumLogProbs(mass, n.getPageRank());
          massMessagesReceived++;
        }
      }

      node.setPageRank(mass);
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      if (structureReceived == 1) {
        context.write(nid, node);
        totalMass = PageRankMetrics.sumLogProbs(totalMass, mass);
      } else if (structureReceived == 0) {
        context.getCounter(PageRank.missingStructure).increment(1);
      } else {
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      out.writeFloat(totalMass);
      out.close();
    }
  }

