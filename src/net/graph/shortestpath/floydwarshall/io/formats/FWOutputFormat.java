package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import net.graph.shortestpath.floydwarshall.FWVertex;
import net.graph.shortestpath.floydwarshall.FWWorkerContext;
import net.graph.shortestpath.floydwarshall.io.FWEdgeValueWritable;
import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.SequenceFileVertexOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordWriter;

public class FWOutputFormat extends FWSequenceFileVertexOutputFormat
	<IntWritable, FWVertexValueWritable, FWEdgeValueWritable, IntWritable, BytesWritable> {

	
	private static int BUF_LENGTH = 512;
	private ByteBuffer path = ByteBuffer.allocate(BUF_LENGTH);


	@Override
	protected void write(RecordWriter<IntWritable, BytesWritable> recordWriter,
			Vertex<IntWritable, FWVertexValueWritable, FWEdgeValueWritable, ?> vertex)
			throws IOException, InterruptedException 
	{
		FWWorkerContext ctx = (FWWorkerContext) vertex.getWorkerContext();
		int idFromDictionary = ctx.getIDFromDictionary("COMMUNICATE");
		int prv, i = vertex.getId().get();
		int[] n_ = vertex.getValue().n().getInts();
		byte[] i_ = vertex.getValue().i().getBytes();
		for (int j=0; j<i_.length; j++) {
			if (i!=j && i_[j]>1) {
				prv = j;
				path.clear();
				path.putInt(idFromDictionary);
				while (prv!=i) {//FWVertex.EMPTY
					path.putInt(prv);
					prv = n_[prv];
				}
				path.flip();
				byte[] dst = new byte[path.limit()];
				path.get(dst);
				
				recordWriter.write(vertex.getId(), new BytesWritable(dst));
			}
		}
		
	}


}
