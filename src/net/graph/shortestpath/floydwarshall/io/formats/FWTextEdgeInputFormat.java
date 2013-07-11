package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;



public class FWTextEdgeInputFormat extends
		TextEdgeInputFormat<IntWritable, IntWritable> {
	
	  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	  @Override
	  public EdgeReader<IntWritable, IntWritable> createEdgeReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new FWTextEdgeReader();
	  }

	  /**
	   * {@link org.apache.giraph.io.EdgeReader} associated with
	   * {@link IntNullTextEdgeInputFormat}.
	   */
	  public class FWTextEdgeReader extends
	      TextEdgeReaderFromEachLineProcessed<IntPair> {
	    @Override
	    protected IntPair preprocessLine(Text line) throws IOException {
	      String[] tokens = SEPARATOR.split(line.toString());
	      return new IntPair(Integer.valueOf(tokens[0]),
	          Integer.valueOf(tokens[1]));
	    }

	    @Override
	    protected IntWritable getSourceVertexId(IntPair endpoints)
	      throws IOException {
	      return new IntWritable(endpoints.getFirst());
	    }

	    @Override
	    protected IntWritable getTargetVertexId(IntPair endpoints)
	      throws IOException {
	      return new IntWritable(endpoints.getSecond());
	    }

	    @Override
	    protected IntWritable getValue(IntPair endpoints) throws IOException {
	      return new IntWritable(0);
	    }
	  }

}
