package uk.ac.bbk.ucl.cw.pagerank.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * 
 * @author santteegt
 *
 */
public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),
    Mass((byte) 1),    
    Structure((byte) 2);

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

	private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
	private int nodeid;
	private float pagerank;
	private ArrayListOfIntsWritable adjacenyList;

	public PageRankNode() {}

	public float getPageRank() {
		return pagerank;
	}

	public void setPageRank(float p) {
		this.pagerank = p;
	}

	public int getNodeId() {
		return nodeid;
	}

	public void setNodeId(int n) {
		this.nodeid = n;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return adjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable list) {
		this.adjacenyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
		nodeid = in.readInt();

		if (type.equals(Type.Mass)) {
			pagerank = in.readFloat();
			return;
		}

		if (type.equals(Type.Complete)) {
			pagerank = in.readFloat();
		}

		adjacenyList = new ArrayListOfIntsWritable();
		adjacenyList.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type.val);
		out.writeInt(nodeid);

		if (type.equals(Type.Mass)) {
			out.writeFloat(pagerank);
			return;
		}

		if (type.equals(Type.Complete)) {
			out.writeFloat(pagerank);
		}

		adjacenyList.write(out);
	}

	@Override
	public String toString() {
		return String.format("{%d %.4f %s}",
				nodeid, pagerank, (adjacenyList == null ? "[]" : adjacenyList.toString(10)));
	}

  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
