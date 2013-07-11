package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;
import java.util.List;

import net.graph.io.TripleWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TripleInputFormat extends EdgeInputFormat<IntWritable, IntWritable> {
	
	protected BinaryInputFormat<NullWritable,TripleWritable> tripleInputFormat = 
			new BinaryInputFormat<NullWritable,TripleWritable>();
	
	@Override
	public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
			throws IOException, InterruptedException {
		return tripleInputFormat.getEdgeSplits(context);
	}
	
	@Override
	public EdgeReader<IntWritable, IntWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new TripleReader();
	}

	protected class TripleReader extends EdgeReader<IntWritable, IntWritable> 
	{
		private RecordReader<NullWritable, TripleWritable> tripleReader;
		private TaskAttemptContext context;

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.context = context;
			tripleReader = createTripleReader(inputSplit, context);
			tripleReader.initialize(inputSplit, context);
		}
		
	    protected RecordReader<NullWritable, TripleWritable>
	    createTripleReader(InputSplit inputSplit, TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return tripleInputFormat.createRecordReader(inputSplit, context);
	    }
	    
	    protected RecordReader<NullWritable, TripleWritable> getRecordReader() {
	        return tripleReader;
	      }

		@Override
		public boolean nextEdge() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}

		@Override
		public IntWritable getCurrentSourceId() throws IOException,
				InterruptedException {
			TripleWritable value = getRecordReader().getCurrentValue();
			return value.getSubject();
		}

		@Override
		public Edge<IntWritable, IntWritable> getCurrentEdge()
				throws IOException, InterruptedException {
			TripleWritable value = getRecordReader().getCurrentValue();
			IntWritable edgeValue = value.getPredicate();
			IntWritable targetVertexId = value.getObject();
			return EdgeFactory.create(targetVertexId, edgeValue);
		}

		@Override
		public void close() throws IOException {
			tripleReader.close();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return tripleReader.getProgress();
		}
		
	    protected TaskAttemptContext getContext() {
	        return context;
	    }
		
	}

}
