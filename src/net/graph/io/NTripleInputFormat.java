package net.graph.io;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class NTripleInputFormat extends TextEdgeInputFormat<LongWritable,NullWritable> 
{

	@Override
	public EdgeReader<LongWritable,NullWritable> createEdgeReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new NTripleReader();
	}
	
	protected class NTripleReader extends TextEdgeInputFormat<LongWritable,NullWritable>.TextEdgeReaderFromEachLineProcessed<NTriple>	
	{
		@Override
		protected LongWritable getSourceVertexId(NTriple nt) throws IOException {
			return nt.getSubject();
		}

		@Override
		protected LongWritable getTargetVertexId(NTriple nt) throws IOException {
			return nt.getObject();
		}

		@Override
		protected NullWritable getValue(NTriple nt) throws IOException {
			return NullWritable.get();//nt.getPredicate();
		}

		@Override
		protected NTriple preprocessLine(Text txt) throws IOException {
			try {
				return new NTriple(txt.toString());
			} catch (NTripleException e) {
				throw new IOException(e);
			}
		}
	}

}
