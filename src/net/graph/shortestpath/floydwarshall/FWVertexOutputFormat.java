package net.graph.shortestpath.floydwarshall;

import java.io.IOException;
import java.text.DecimalFormat;

import net.graph.adjlist.AdjListWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FWVertexOutputFormat extends TextVertexOutputFormat<IntWritable, AdjListWritable, NullWritable> 
{
	private static DecimalFormat myFormatter = new DecimalFormat("000.000000000");
  
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new VertexLongWriter();
	}

	public class VertexLongWriter extends TextVertexWriter 
	{
		@Override
		public void writeVertex(Vertex<IntWritable, AdjListWritable, NullWritable, ?> vertex) throws IOException, InterruptedException 
		{
			StringBuilder sb = new StringBuilder();			
			AdjListWritable i_ = vertex.getValue();
			int i = vertex.getId().get();
			sb.append(i).append("\t");		
			sb.append(myFormatter.format(i_.avg())).append("\t");
			sb.append(myFormatter.format(i_.sum())).append("\t");
			sb.append(i_);
			getRecordWriter().write(new Text(sb.toString()),null);
		}
	}

}
