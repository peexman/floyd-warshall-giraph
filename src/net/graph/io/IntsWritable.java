package net.graph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

public class IntsWritable implements Writable
{
	private static final Log LOG = LogFactory.getLog(IntsWritable.class);
	private static final int LENGTH_INTS = 4;
	private static final int[] EMPTY_INTS = {};
	
	private int size;
	private int[] ints;
	
	public IntsWritable() {this(EMPTY_INTS);}
	
	public IntsWritable(int[] ints) {
		this.ints = ints;
		this.size = ints.length;
	}

	public int[] getInts() {
		return ints;
	}

	public int getLength() {
		return size;
	}
	
	/**
	* Change the size of the buffer. The values in the old range are preserved
	* and any new values are undefined. The capacity is changed if it is 
	* necessary.
	* @param size The new number of bytes
	*/
	public void setSize(int size) {
		if (size > getCapacity()) {
		  setCapacity(size * 3 / 2);
		}
		this.size = size;
	}
	
	/**
	* Get the capacity, which is the maximum size that could handled without
	* resizing the backing storage.
	* @return The number of bytes
	*/
	public int getCapacity() {
		return ints.length;
	}
	
	/**
	* Change the capacity of the backing storage.
	* The data is preserved.
	* @param new_cap The new capacity in bytes.
	*/
	public void setCapacity(int new_cap) {
	if (new_cap != getCapacity()) {
	  int[] new_data = new int[new_cap];
	  if (new_cap < size) {
	    size = new_cap;
	  }
	  if (size != 0) {
	    System.arraycopy(ints, 0, new_data, 0, size);
	  }
	  ints = new_data;
	}
	}
	
	/**
	* Set the BytesWritable to the contents of the given newData.
	* @param newData the value to set this BytesWritable to.
	*/
	public void set(IntsWritable newData) {
		set(newData.ints, 0, newData.size);
	}
	
	/**
	* Set the value to a copy of the given byte range
	* @param newData the new values to copy in
	* @param offset the offset in newData to start at
	* @param length the number of bytes to copy
	*/
	public void set(int[] newData, int offset, int length) {
		setSize(0);
		setSize(length);
		System.arraycopy(newData, offset, ints, 0, size);
	}
	
	// inherit javadoc
	public void readFields(DataInput in) throws IOException {
		setSize(in.readInt());
		for (int i = 0; i < size; i++)
		  ints[i] = in.readInt(); 
	}
	
	// inherit javadoc
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for (int i = 0; i < size; i++)
			  out.writeInt(ints[i]); 
	}
	
	public int hashCode() {
		return super.hashCode();
	}
	
	/**
	* Are the two byte sequences equal?
	*/
	public boolean equals(Object right_obj) {
	if (right_obj instanceof IntsWritable)
	  return super.equals(right_obj);
	return false;
	}
	
	/**
	* Generate the stream of bytes as hex pairs separated by ' '.
	*/
	public String toString() { 
	StringBuffer sb = new StringBuffer(3*size);
	for (int idx = 0; idx < size; idx++) {
	  // if not the first, put a blank separator in
	  if (idx != 0) {
	    sb.append(' ');
	  }
	  String num = Integer.toHexString(0xffff & ints[idx]);
	  // if it is only one digit, add a leading 0.
//	  if (num.length() < 2) {
//	    sb.append('0');
//	  }
	  sb.append(num);
	}
	return sb.toString();
	}

}
