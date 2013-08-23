package net.graph.shortestpath.floydwarshall.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class FWEdgeValueWritable implements Writable {
	
	private IntWritable relation;
	private IntWritable guid;
	private IntWritable idx;

	public IntWritable getRelation() {
		return relation;
	}
		
	public IntWritable getGuid() {
		return guid;
	}

	public IntWritable getIdx() {
		return idx;
	}

	public FWEdgeValueWritable() {
		set(new IntWritable(), new IntWritable(), new IntWritable());
	}
	
	public FWEdgeValueWritable(int relation, int guid, int idx) {
		set(new IntWritable(relation), new IntWritable(guid), new IntWritable(idx));
	}
	
	public FWEdgeValueWritable(IntWritable relation, IntWritable guid, IntWritable idx) {
		set(relation, guid,idx);
	}

	public void set(IntWritable relation, IntWritable guid, IntWritable idx) {
		this.relation = relation;
		this.guid = guid;
		this.idx = idx;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		relation.write(out);
		guid.write(out);
		idx.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		relation.readFields(in);
		guid.readFields(in);
		idx.readFields(in);
	}


}
