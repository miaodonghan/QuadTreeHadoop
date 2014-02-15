package mapReduceTasks;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

import quadIndex.QuadTree;
import quadIndex.SpatialObj;


public class QuadTreeWritable implements Writable{

	private QuadTree quadtree = new QuadTree();
	private byte[] raw = null;
	private ByteArrayOutputStream bos = new ByteArrayOutputStream();
	private DataOutputStream rawdata = new DataOutputStream(bos);
	private long offset = 0L;
	
	final static int maxBlockSize = 63*1024*1024;
	
	// returns value indicating if more records can be inserted.
	
	public boolean insert(SpatialObj obj) throws IOException{
		quadtree.insert(obj, offset);
		offset+=obj.size();
		obj.write(rawdata);
		return isAlmostFull();
	}
	
	private int size(){
		return quadtree.size() + rawdata.size()*8;
	}
	
	private boolean isAlmostFull(){
		return size() >= maxBlockSize;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		quadtree.readFields(in);
		int size = in.readInt();
		raw = new byte[size];
		in.readFully(raw);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		quadtree.write(out);
		out.writeInt(bos.size());
		raw = bos.toByteArray();
		out.write(bos.toByteArray());
	}
}
