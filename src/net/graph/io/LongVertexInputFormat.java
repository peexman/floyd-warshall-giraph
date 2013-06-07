package net.graph.io;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.ImmutableList;

public class LongVertexInputFormat extends TextVertexInputFormat<LongWritable, MapWritable, NullWritable> {
	  @Override
	  public TextVertexReader createVertexReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new LongVertexReader();
	  }

	  /**
	   * Reader for this InputFormat.
	   */
	  public class LongVertexReader extends TextVertexReaderFromEachLineProcessed<String> {
	    /** Cached vertex id */
	    private LongWritable id;

	    @Override
	    protected String preprocessLine(Text line) throws IOException {
	      id = new LongWritable(Long.parseLong(line.toString()));
	      return line.toString();
	    }

	    @Override
	    protected LongWritable getId(String line) throws IOException {
	      return id;
	    }

	    @Override
	    protected MapWritable getValue(String line) throws IOException {
	      return new MapWritable();
	    }

	    @Override
	    protected Iterable<Edge<LongWritable, NullWritable>> getEdges(String line)
	      throws IOException {
	      return ImmutableList.of();
	    }
	  }
}
