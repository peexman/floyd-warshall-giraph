package net.graph.adjlist;

import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class LongIntAdjList
{
	private MapWritable adj_list = null;
	private long id;
	
	public LongIntAdjList(long id) {
		adj_list = new MapWritable();
		this.id = id;
	}
	
	public LongIntAdjList(long id, MapWritable map) {
		adj_list = new MapWritable(map);
		this.id = id;
	}

	public void aggregate(LongIntAdjList new_k) {
		if (new_k.adj_list.size()>0)
			this.adj_list = new_k.adj_list;
	}
	
	public int sum()
	{
		int sum=0;
		Collection<Writable> values = adj_list.values();
		Iterator<Writable> it = values.iterator();
		while (it.hasNext()) {
			IntWritable d = (IntWritable) it.next();
			sum += d.get();
		}
		return sum;
	}

	public Integer get(long j)
	{
		if (id==j) return 0;
		IntWritable value = (IntWritable) adj_list.get(new LongWritable(j));			
		return (value==null) ? null : value.get();
	}

	public void set(long j, int v)
	{
		if (id==j) return;
		LongWritable J = new LongWritable(j);
		IntWritable V = new IntWritable(v);
		adj_list.put(J, V);
	}
	
	public String toString(long V)
	{		
		StringBuilder sb = new StringBuilder();
		for (long j=0; j<V; j++) {
			Integer value = get(j);
			sb.append((value==null)?"-":value);
			sb.append("\t");
		}
		return sb.toString();
	}
	
	public MapWritable getMap() {
		return adj_list;
	}

	public Float avg(long v) {
		if (v==0) return null;
		return (float)sum()/(v*(v-1));
	}

}
