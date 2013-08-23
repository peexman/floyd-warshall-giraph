package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;
import java.util.List;

import net.graph.io.QuadWritable;
import net.graph.shortestpath.floydwarshall.io.FWEdgeValueWritable;

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

public class QuadInputFormat extends EdgeInputFormat<IntWritable, FWEdgeValueWritable> {
	
	protected BinaryInputFormat<NullWritable,QuadWritable> tripleInputFormat = 
			new BinaryInputFormat<NullWritable,QuadWritable>();
	
	@Override
	public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
			throws IOException, InterruptedException {
		return tripleInputFormat.getEdgeSplits(context);
	}
	
	@Override
	public EdgeReader<IntWritable, FWEdgeValueWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new QuadReader();
	}

	protected class QuadReader extends EdgeReader<IntWritable, FWEdgeValueWritable> 
	{
		private RecordReader<NullWritable, QuadWritable> tripleReader;
		private TaskAttemptContext context;

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.context = context;
			tripleReader = createTripleReader(inputSplit, context);
			tripleReader.initialize(inputSplit, context);
		}
		
	    protected RecordReader<NullWritable, QuadWritable>
	    createTripleReader(InputSplit inputSplit, TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return tripleInputFormat.createRecordReader(inputSplit, context);
	    }
	    
	    protected RecordReader<NullWritable, QuadWritable> getRecordReader() {
	        return tripleReader;
	      }

		@Override
		public boolean nextEdge() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}

		@Override
		public IntWritable getCurrentSourceId() throws IOException,
				InterruptedException {
			QuadWritable value = getRecordReader().getCurrentValue();
			return value.getSubject();
		}

		@Override
		public Edge<IntWritable, FWEdgeValueWritable> getCurrentEdge()
				throws IOException, InterruptedException {
			QuadWritable value = getRecordReader().getCurrentValue();
			FWEdgeValueWritable edgeValue = new FWEdgeValueWritable(value.getPredicate(), 
					value.getGUID(), value.getIDX());
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
