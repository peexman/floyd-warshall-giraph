package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public abstract class FWSequenceFileVertexOutputFormat <
	  I extends WritableComparable,
	  V extends Writable,
	  E extends Writable,
	  OK extends Writable,
	  OV extends Writable>
	  extends VertexOutputFormat<I, V, E> {
	  /**
	   * Output format of a sequence file that stores key-value pairs of the
	   * desired types.
	   */
	  private SequenceFileOutputFormat<OK, OV> sequenceFileOutputFormat =
	      new SequenceFileOutputFormat<OK, OV>();

	  @Override
	  public void checkOutputSpecs(JobContext context)
	    throws IOException, InterruptedException {
	    sequenceFileOutputFormat.checkOutputSpecs(context);
	  }

	  @Override
	  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
	    throws IOException, InterruptedException {
	    return sequenceFileOutputFormat.getOutputCommitter(context);
	  }

	  @Override
	  public VertexWriter createVertexWriter(TaskAttemptContext
	      context) throws IOException, InterruptedException {
	    return new SequenceFileVertexWriter();
	  }

	  protected abstract void write(RecordWriter<OK, OV> recordWriter, Vertex<I, V, E, ?> vertex) throws
	      IOException, InterruptedException;

	  /**
	   * Vertex writer that converts a vertex into a key-value pair and writes
	   * the result into a sequence file for a context.
	   */
	  private class SequenceFileVertexWriter extends VertexWriter<I, V, E> {
	    /**
	     * A record writer that will write into a sequence file initialized for
	     * a context.
	     */
	    private RecordWriter<OK, OV> recordWriter;

	    @Override
	    public void initialize(TaskAttemptContext context) throws IOException,
	           InterruptedException {
	      recordWriter = sequenceFileOutputFormat.getRecordWriter(context);
	    }

	    @Override
	    public final void writeVertex(Vertex<I, V, E, ?> vertex) throws
	      IOException, InterruptedException {
	    	write(recordWriter, vertex);
	    }

	    @Override
	    public void close(TaskAttemptContext context) throws IOException,
	        InterruptedException {
	      recordWriter.close(context);
	    }
	  }
	}

