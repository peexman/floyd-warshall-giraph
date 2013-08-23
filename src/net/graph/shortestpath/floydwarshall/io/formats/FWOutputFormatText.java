package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;

import net.graph.shortestpath.floydwarshall.FWVertex;
import net.graph.shortestpath.floydwarshall.FWWorkerContext;
import net.graph.shortestpath.floydwarshall.io.FWEdgeValueWritable;
import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FWOutputFormatText extends TextVertexOutputFormat
	<IntWritable, FWVertexValueWritable, FWEdgeValueWritable> {

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new VertexLongWriter();
	}

	public class VertexLongWriter extends TextVertexWriter 
	{
		
		@Override
		public void writeVertex(Vertex<IntWritable, FWVertexValueWritable, FWEdgeValueWritable, ?> vertex) 
				throws IOException, InterruptedException 
		{	
			FWWorkerContext ctx = (FWWorkerContext) vertex.getWorkerContext();
			int idFromDictionary = ctx.getIDFromDictionary("COMMUNICATE");			
			int prv, i = vertex.getId().get();
			int[] n_ = vertex.getValue().n().getInts();
			byte[] i_ = vertex.getValue().i().getBytes();
			for (int j=0; j<i_.length; j++) {
				StringBuilder sb = new StringBuilder();
				if (i!=j && i_[j]>1) {				
					prv = j;
					sb.append(idFromDictionary).append("\t");
					while (prv!=i) {
						sb.append(prv).append("\t");
						prv = n_[prv];
					}
					
					getRecordWriter().write(new Text(String.valueOf(vertex.getId())), 
							new Text(sb.toString()));
				}
			}

		}
	}


}
