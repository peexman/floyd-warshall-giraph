package net.graph.shortestpath.floydwarshall.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import net.graph.io.IntsWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class AllPathsWritable implements Writable 
{
	private IntsWritable[] sources;

	public AllPathsWritable(int size) {
		sources = new IntsWritable[size];
	}
	
	public int[] get(int source, int dest) {
		IntsWritable paths = (IntsWritable) sources[source];
		int[] ints=paths.getInts();
		int pos = 0;
		while (pos<ints.length) {
			int pathlength = ints[pos++];
			if (ints[pos]==dest) {
				int[] path = new int[pathlength];				
				for (int i=path.length-1; i>=0; i--) {
					path[i]=ints[pos++];
				}
				return path;
			} else {
				pos += pathlength;
			}
		}
		return null;
	}
	
	public void add(IntWritable source, IntsWritable path) {
		IntsWritable paths = (IntsWritable) sources[source.get()];
		IntBuffer inbuf = IntBuffer.allocate(paths.getLength()+path.getLength()+1);
		inbuf.put(path.getLength());
		inbuf.put(path.getInts());
		sources[source.get()] = new IntsWritable(inbuf.array());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sources = new IntsWritable[in.readInt()];
		for (int i=0; i<sources.length; i++) {
			sources[i].readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(sources.length);
		for (int i=0; i<sources.length; i++) {
			sources[i].write(out);
		}
	}

}
