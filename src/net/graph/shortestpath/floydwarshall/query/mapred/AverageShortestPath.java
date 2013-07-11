package net.graph.shortestpath.floydwarshall.query.mapred;

import java.io.IOException;
import java.util.Iterator;

import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageShortestPath {

	public static class Map extends Mapper<IntWritable, FWVertexValueWritable, NullWritable, IntWritable>
	{
		@Override
		protected void map(IntWritable key, FWVertexValueWritable value, Context context)
				throws IOException, InterruptedException {
			
			byte[] bytes = value.i().getBytes();
			
			int sum=0;
			for (int i=0; i<bytes.length; i++) {
				sum += bytes[i];
			}
			
			context.write(NullWritable.get(), new IntWritable(sum));
		}
	}
	
	public static class Reduce extends Reducer<NullWritable, IntWritable, IntWritable, IntWritable>
	{

		@Override
		protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int V=0,sum=0;
			Iterator<IntWritable> it = values.iterator();
			while (it.hasNext()) {
				IntWritable intWritable = it.next();
				sum += intWritable.get();
				V++;
			}
			
			context.write(new IntWritable(V), new IntWritable(sum));
		}
		
	}

}
