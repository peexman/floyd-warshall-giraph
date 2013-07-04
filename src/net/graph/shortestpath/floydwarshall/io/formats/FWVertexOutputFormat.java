package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;
import java.text.DecimalFormat;

import net.graph.io.IntsWritable;
import net.graph.shortestpath.floydwarshall.FWVertex;
import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FWVertexOutputFormat extends TextVertexOutputFormat<IntWritable, FWVertexValueWritable, IntWritable> 
{
	private static DecimalFormat myFormatter = new DecimalFormat("000.000000000");
  
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new VertexLongWriter();
	}

	public class VertexLongWriter extends TextVertexWriter 
	{
		public int sum(byte[] bytes)
		{
			int sum=0;
			for (int j=0; j<bytes.length; j++)
				sum += bytes[j]==FWVertex.INFINITY?0:bytes[j];
			return sum;
		}
		
		@Override
		public void writeVertex(Vertex<IntWritable, FWVertexValueWritable, IntWritable, ?> vertex) throws IOException, InterruptedException 
		{	
			int i = vertex.getId().get();
			
			BytesWritable i_ = vertex.getValue().i();
			IntsWritable n_ = vertex.getValue().n();
			
			StringBuilder sb = new StringBuilder();
			sb.append(i).append("\t");		
			sb.append(myFormatter.format(sum(i_.getBytes()))).append("\t");
			sb.append(i_).append("\t").append(n_);
			getRecordWriter().write(new Text(sb.toString()),null);
		}
	}

}
