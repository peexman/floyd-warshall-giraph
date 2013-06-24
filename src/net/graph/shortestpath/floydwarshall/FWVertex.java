package net.graph.shortestpath.floydwarshall;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import net.graph.adjlist.AdjListWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

public class FWVertex extends Vertex<IntWritable, AdjListWritable, NullWritable, NullWritable> 
{
	private static Logger LOG = Logger.getLogger(FWVertex.class);
	private byte[] i_, k_;
	private int i, k, V;
	
	@Override
	public void compute(Iterable<NullWritable> messages) throws IOException 
	{
		V = (int) getTotalNumVertices();
		long s = getSuperstep();
		i = getId().get();
		i_ = k_ = null;		
		k = (int) s-1;

		if (k<0) setup(); else compute();
	
		if (i==s) aggregate(FWAggregator.ID, getValue());
		if (V==s) voteToHalt();
	}
	
	private void compute() throws IOException
	{
		i_ = getValue().getBytes();
		if (i_[k]!=AdjListWritable.INFINITY) 
		{
			FWWorkerContext ctx = (FWWorkerContext) getWorkerContext();
			ExecutorService pool = ctx.getPool();
			k_ = ctx.getMap().getBytes();
			try 
			{
				ArrayList<Compute> clist = initPool(ctx);
				getContext().progress();
				pool.invokeAll(clist);
			} 
			catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
		LOG.debug("(k,i)=("+k+","+i+") i_="+i_);
	}
	
	private ArrayList<Compute> initPool(FWWorkerContext ctx)
	{
		ArrayList<Compute> computeUnits = ctx.getComputeUnits(i);		
		if (computeUnits==null) {
			int start, stop;
			int offset = ctx.getOffset();		
			computeUnits = new ArrayList<Compute>();			
			for (int n=0; n<ctx.getComputeThreads(); n++) {
				start = offset*n;
				stop = start+offset;
				if (stop>V) stop=V;
				computeUnits.add(new Compute(start,stop));
			}
			ctx.setComputeUnits(i,computeUnits);
		}
		return computeUnits;
	}

	private void setup()
	{
		byte[] i_ = new byte[V];
		Arrays.fill(i_, 0, V, AdjListWritable.INFINITY);
		i_[i]=0;
		for (Edge<IntWritable, NullWritable> edge : getEdges()) {
			int j = edge.getTargetVertexId().get();
			if (i!=j) i_[j]=1;			
		}
		setValue(new AdjListWritable(i_));
	}
	
	public class Compute implements Callable<Integer>
	{
		private final int start;
		private final int stop;
		
		public Compute(int start, int stop) {
			this.start = start;
			this.stop = stop;
		}

		@Override
		public Integer call() 
		{
			for (int j=start; j<stop; j++) 
			{
				if (i==j) continue;
				if (k_[j]!=AdjListWritable.INFINITY) {
					byte sum = (byte) (i_[k]+k_[j]);
					if (i_[j]==AdjListWritable.INFINITY) {
						i_[j]=sum;
					} else {
						if (sum<i_[j]) {
							i_[j]=sum;
//							n_[j]=k;
						}
					}
				}
			}
			return 0;
		}
		
	}
	
}
