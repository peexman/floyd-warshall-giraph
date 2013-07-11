package net.graph.shortestpath.floydwarshall.io.formats;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.io.formats.SequenceFileVertexOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

public class FWOutputFormat extends SequenceFileVertexOutputFormat
	<IntWritable, FWVertexValueWritable, IntWritable, IntWritable, FWVertexValueWritable> {

	@Override
	protected IntWritable convertToSequenceFileKey(IntWritable vertexId) {
		return vertexId;
	}

	@Override
	protected FWVertexValueWritable convertToSequenceFileValue(FWVertexValueWritable vertexValue) 
	{
//		byte[] bytes = vertexValue.i().getBytes();
//		int[] ints = vertexValue.n().getInts();
//		
//		ByteBuffer outbuffer_b = ByteBuffer.allocate(bytes.length*5);
//		outbuffer_b.put(bytes);
//		
//		IntBuffer outbuffer_i = outbuffer_b.asIntBuffer();
//		outbuffer_i.put(ints);
//		
//		return new BytesWritable(outbuffer_b.array());
		return vertexValue;
	}


}
