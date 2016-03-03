package uk.ac.bbk.ucl.cw.util;

import it.unimi.dsi.fastutil.objects.Object2FloatMap;
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AssociativeArray extends Object2FloatOpenHashMap<String> implements Writable{

	private static final long serialVersionUID = 1L;
	
    @Override
	public void readFields(DataInput in) throws IOException {
		clear();
		
		float numEntries = in.readFloat();
		if (numEntries == 0) {
			return;
		}
		for (int i = 0; i < numEntries; i++) {
			String k = in.readUTF();
			float v = in.readFloat();
			super.put(k, v);
		}
	}

    @Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(size());
		if (size() == 0) {
			return;
		}
		
		for (Object2FloatMap.Entry<String> e : object2FloatEntrySet()) {
			out.writeUTF((String)e.getKey());
			out.writeFloat(((Float)e.getValue()).floatValue());
		}
	}

	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);
		return bytesOut.toByteArray();
	}	

	public static AssociativeArray create(DataInput in) throws IOException {
		AssociativeArray m = new AssociativeArray();
		m.readFields(in);
		return m;
	}
	
	public static AssociativeArray create(byte[] bytes) throws IOException
	{
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	public void sum(AssociativeArray m) {
		for (Object2FloatMap.Entry<String> e : m.object2FloatEntrySet()) {
			String key = (String)e.getKey();
			
			if (containsKey(key)) {
				put(key, get(key).floatValue() + ((Float)e.getValue()).floatValue());
			} else {
				put(key, (Float)e.getValue());
			}
		}
	}
	
	public void getRelativeFrequency(AssociativeArray m) {
		sum(m);
		Float relativeFreq = 0.0f;
		for (Object2FloatMap.Entry<String> e : m.object2FloatEntrySet()) {
			String key = (String)e.getKey();
			
			if (containsKey(key) && !key.equals("*")) {
				put(key, get(key).floatValue() + ((Float)e.getValue()).floatValue());
			} else if (containsKey(key) && key.equals("*")) {
				relativeFreq += ((Float)e.getValue()).floatValue();
				//put(key, get(key).floatValue() + ((Float)e.getValue()).floatValue());
			} else {
				put(key, (Float)e.getValue());
			}
		}
		for (Object2FloatMap.Entry<String> array: this.object2FloatEntrySet()) {
			String key = array.getKey(); 
			put(key, get(key).floatValue() / relativeFreq);
		}
		
	}

//	public int dot(AssociativeArray m) {
//		int s = 0;
//		for (Object2FloatMap.Entry<String> e : m.object2FloatEntrySet()) {
//			String key = (String)e.getKey();
//			if (containsKey(key)) {
//				s += get(key).floatValue() * ((Float)e.getValue()).floatValue();
//			}
//		}
//		return s;
//	}	

	public void increment(String key)
	{
		increment(key, 1);
	}

	public void increment(String key, float n)
	{
		if (containsKey(key)) {
			put(key, get(key).floatValue() + n);
		} else {
			put(key, n);
		}
	}
}