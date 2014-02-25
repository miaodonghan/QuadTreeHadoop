package quadIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Writable;
/*
 * This index data describing the location of a 
 * spatial obj in the raw-data file.
 * Author: Donghan Miao
 * */
public class FileLoc implements Writable {

	int offset;
	int length;
	
	public long getOffset() {
		return offset;
	}

	public int getLength() {
		return length;
	}

	public FileLoc(int off, int len) {
		offset = off;
		length = len;
	}

	public FileLoc() {
		offset = -1;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		offset = in.readInt();
		length = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(offset);
		out.writeInt(length);
	}

	public String toString() {
		return String.valueOf(offset) + ":" + String.valueOf(length + ";");

	}

	// ensure the uniqueness of the object found,
	// therefore, minimize network flows.
	public int hashCode() {
		return new HashCodeBuilder(17, 31).
		// two randomly chosen prime numbers
		// if deriving: appendSuper(super.hashCode()).
		append(offset).append(length).toHashCode();
	}

	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj == this)
			return true;
		if (!(obj instanceof FileLoc))
			return false;

		FileLoc rhs = (FileLoc) obj;
		return new EqualsBuilder()
				.
				// if deriving: appendSuper(super.equals(obj)).
				append(offset, rhs.offset).append(length, rhs.length)
				.isEquals();
	}
	
	public static int size(){
		return 4*2;
	}
}