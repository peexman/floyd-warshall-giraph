package net.graph.shortestpath.floydwarshall;

import java.io.IOException;

import net.graph.adjlist.AdjListWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.ImmutableList;

public class FWVertexInputFormat extends TextVertexInputFormat<IntWritable, AdjListWritable, NullWritable> {
	  @Override
	  public TextVertexReader createVertexReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new IntVertexReader();
	  }

	  /**
	   * Reader for this InputFormat.
	   */
	  public class IntVertexReader extends TextVertexReaderFromEachLine {

		@Override
		protected IntWritable getId(Text line) throws IOException {
			return new IntWritable(Integer.parseInt(line.toString()));
		}

		@Override
		protected AdjListWritable getValue(Text line) throws IOException {
			return new AdjListWritable();
		}

		@Override
		protected Iterable<Edge<IntWritable, NullWritable>> getEdges(Text line)
				throws IOException {
			return ImmutableList.of();
		}

	  }
}
