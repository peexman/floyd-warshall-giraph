package net.graph.shortestpath.floydwarshall;

import java.io.IOException;
import java.text.DecimalFormat;

import net.graph.adjlist.LongIntAdjList;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FWVertexOutputFormat extends TextVertexOutputFormat<LongWritable, MapWritable, NullWritable> 
{
	private static DecimalFormat myFormatter = new DecimalFormat("000.000000000");
  
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new VertexLongWriter();
	}

	public class VertexLongWriter extends TextVertexWriter 
	{
		@Override
		public void writeVertex(Vertex<LongWritable, MapWritable, NullWritable, ?> vertex) throws IOException, InterruptedException 
		{
			StringBuilder sb = new StringBuilder();
			long V = vertex.getTotalNumVertices();
			long id = vertex.getId().get();
			LongIntAdjList i_ = new LongIntAdjList(id,vertex.getValue());
			sb.append(id).append("\t");		
			sb.append(myFormatter.format(i_.avg(V))).append("\t");
			sb.append(i_.toString(V));
			getRecordWriter().write(new Text(sb.toString()),null);
		}
	}

}