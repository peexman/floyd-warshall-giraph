package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;
import java.util.regex.Pattern;

import net.graph.shortestpath.floydwarshall.io.FWEdgeValueWritable;
import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.ImmutableList;

public class FWTextVertexInputFormat extends TextVertexInputFormat<IntWritable, FWVertexValueWritable, FWEdgeValueWritable> {
	  @Override
	  public TextVertexReader createVertexReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new IntVertexReader();
	  }

	  /**
	   * Reader for this InputFormat.
	   */
	  public class IntVertexReader extends TextVertexReaderFromEachLine {
		  
		private final Pattern SEPARATOR = Pattern.compile("\t");

		@Override
		protected IntWritable getId(Text line) throws IOException {
			String[] split = SEPARATOR.split(line.toString());
			return new IntWritable(Integer.parseInt(split[1]));
		}

		@Override
		protected FWVertexValueWritable getValue(Text line) throws IOException {
			return new FWVertexValueWritable();
		}

		@Override
		protected Iterable<Edge<IntWritable, FWEdgeValueWritable>> getEdges(Text line)
				throws IOException {
			return ImmutableList.of();
		}

	  }
}
