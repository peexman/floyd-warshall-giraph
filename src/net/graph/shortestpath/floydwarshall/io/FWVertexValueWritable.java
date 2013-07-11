package net.graph.shortestpath.floydwarshall.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.graph.io.IntsWritable;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class FWVertexValueWritable implements Writable {
	
	private BytesWritable i;
	private IntsWritable n;

	public FWVertexValueWritable() {
		set(new BytesWritable(), new IntsWritable());
	}
	
	public FWVertexValueWritable(byte[] i, int[] n) {
		set(new BytesWritable(i), new IntsWritable(n));
	}
	
	public FWVertexValueWritable(BytesWritable i, IntsWritable n) {
		set(i,n);
	}

	public BytesWritable i() { return i; }
	public IntsWritable n() { return n; }
	
	public void aggregate(FWVertexValueWritable newData) {
		if (newData.i.getLength()>0) {
			i.set(newData.i);
			n.set(newData.n);
		}
	}
	
	public void set(BytesWritable i, IntsWritable n) {
		this.i = i;
		this.n = n;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		i.write(out);
		n.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		i.readFields(in);
		n.readFields(in);
	}


}
