package net.graph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class QuadWritable extends TripleWritable {
	
	private IntWritable guid;
	private IntWritable idx;

	public QuadWritable() {
		guid = new IntWritable();
		idx = new IntWritable();
	}
	
	public QuadWritable(int guid, int subject, int predicate, int object, int idx) {
		set(new IntWritable(guid),
				new IntWritable(subject),
				new IntWritable(predicate),
				new IntWritable(object),
				new IntWritable(idx)
		);
	}
	
	public QuadWritable(IntWritable guid, 
			IntWritable subject, IntWritable predicate, 
			IntWritable object, IntWritable idx) {
		set(guid, subject, predicate, object, idx);
	}

	public void set(IntWritable guid, 
			IntWritable subject, IntWritable predicate, 
			IntWritable object, IntWritable idx) {
		super.set(subject, predicate, object);
		this.guid = guid;
		this.idx = idx;
	}

	public IntWritable getGUID() { return guid; }
	public IntWritable getIDX() { return idx; }
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());		
		sb.append("\t").append(guid).append(":G\t").append(idx).append(":I");
		return sb.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		guid.readFields(in);
		idx.readFields(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		guid.write(out);
		idx.write(out);		
	}

}
