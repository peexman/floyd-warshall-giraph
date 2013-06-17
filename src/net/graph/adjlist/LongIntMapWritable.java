package net.graph.adjlist;

import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class LongIntMapWritable extends MapWritable
{
	synchronized public Integer get(long j)
	{
		IntWritable value = (IntWritable) get(new LongWritable(j));			
		return value==null?null:value.get();
	}

	synchronized public void set(long j, int v)
	{
		LongWritable J = new LongWritable(j);
		IntWritable V = new IntWritable(v);
		put(J, V);
	}
	
	public int sum()
	{
		int sum=0;
		Collection<Writable> values = values();
		Iterator<Writable> it = values.iterator();
		while (it.hasNext()) {
			IntWritable d = (IntWritable) it.next();
			sum += d.get();
		}
		return sum;
	}
	
	public Float avg(long V) {
		if (V==0) return null;
		return (float)sum()/(V*(V-1));
	}
	
	public String toString(long i, long V)
	{		
		StringBuilder sb = new StringBuilder();
		for (long j=0; j<V; j++) {
			Integer value = i==j?(Integer)0:get(j);
			sb.append(value==null?"-":value);
			sb.append("\t");
		}
		return sb.toString();
	}
}
