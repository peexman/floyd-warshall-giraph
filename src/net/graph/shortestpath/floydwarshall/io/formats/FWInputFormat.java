package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;
import java.util.List;

import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.io.VertexValueInputFormat;
import org.apache.giraph.io.VertexValueReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FWInputFormat extends VertexValueInputFormat<IntWritable, FWVertexValueWritable> {
	
	protected BinaryInputFormat<IntWritable,FWVertexValueWritable> fwVVInputFormat = 
			new BinaryInputFormat<IntWritable,FWVertexValueWritable>();
	
	@Override
	public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
			throws IOException, InterruptedException {
		return fwVVInputFormat.getVertexSplits(context);
	}
	
	@Override
	public VertexValueReader<IntWritable, FWVertexValueWritable> createVertexValueReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new Reader();
	}

	protected class Reader extends VertexValueReader<IntWritable, FWVertexValueWritable> 
	{
		private RecordReader<IntWritable, FWVertexValueWritable> reader;
		private TaskAttemptContext context;

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			
			this.context = context;
			reader = createReader(inputSplit, context);
			reader.initialize(inputSplit, context);
		}
		
	    protected RecordReader<IntWritable, FWVertexValueWritable>
	    createReader(InputSplit inputSplit, TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return fwVVInputFormat.createRecordReader(inputSplit, context);
	    }
	    
	    protected RecordReader<IntWritable, FWVertexValueWritable> getRecordReader() {
	        return reader;
	      }

		@Override
		public void close() throws IOException {
			getRecordReader().close();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return getRecordReader().getProgress();
		}
		
	    protected TaskAttemptContext getContext() {
	        return context;
	    }

		@Override
		public IntWritable getCurrentVertexId() throws IOException, InterruptedException {
			return getRecordReader().getCurrentKey();
		}

		@Override
		public FWVertexValueWritable getCurrentVertexValue() throws IOException,
				InterruptedException {
			return getRecordReader().getCurrentValue();
		}

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}
		
	}



}
