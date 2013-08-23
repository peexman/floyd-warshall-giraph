package net.graph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class TripleWritable implements Writable {
	
	private IntWritable subject;
	private IntWritable predicate;
	private IntWritable object;

	public TripleWritable() {
		subject = new IntWritable();
		predicate = new IntWritable();
		object = new IntWritable();
	}
	
	public TripleWritable(int subject, int predicate, int object) {
		set(new IntWritable(subject),new IntWritable(predicate),new IntWritable(object));
	}
	
	public TripleWritable(IntWritable subject, IntWritable predicate, IntWritable object) {
		set(subject, predicate, object);
	}

	public void set(IntWritable subject, IntWritable predicate, IntWritable object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	public IntWritable getSubject() { return subject; }
	public IntWritable getPredicate() { return predicate; }
	public IntWritable getObject() { return object; }
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(subject).append(":S\t").append(predicate).append(":P\t").append(object).append(":O");
		return sb.toString();
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		subject.readFields(in);
		predicate.readFields(in);
		object.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		subject.write(out);
		predicate.write(out);
		object.write(out);
	}

}
