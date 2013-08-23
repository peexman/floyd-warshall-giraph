package net.graph.shortestpath.floydwarshall;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import net.graph.shortestpath.floydwarshall.io.FWEdgeValueWritable;
import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

public class FWVertex extends Vertex<IntWritable, FWVertexValueWritable, FWEdgeValueWritable, NullWritable> 
{
	private static Logger LOG = Logger.getLogger(FWVertex.class);
	
	public static final byte INFINITY = Byte.MIN_VALUE;
	public static final int EMPTY = Integer.MIN_VALUE;
	private byte[] i_, k_;
	private int i, k, V;
	private int[] n_, pk_;
	
	@Override
	public void compute(Iterable<NullWritable> messages) throws IOException 
	{
		V = (int) getTotalNumVertices();
		long s = getSuperstep();
		i = getId().get();
		i_ = k_ = null;		
		k = (int) s-1;
		n_ = null;

		if (k<0) setup(); else compute();
	
		if (i==s) aggregate(FWAggregator.ID, getValue());
		if (V==s) voteToHalt();
	}
	
	private void compute() throws IOException
	{
		i_ = getValue().i().getBytes();
		if (i_[k]!=INFINITY) 
		{
			n_ = getValue().n().getInts();
			
			FWWorkerContext ctx = (FWWorkerContext) getWorkerContext();
			ExecutorService pool = ctx.getPool();
			k_ = ctx.getPrevious().i().getBytes();
			pk_ = ctx.getPrevious().n().getInts();
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
		int[] n_ = new int[V];
		byte[] i_ = new byte[V];
		Arrays.fill(i_, 0, V, INFINITY);
		Arrays.fill(n_, 0, V, EMPTY);
		FWWorkerContext ctx = (FWWorkerContext) getWorkerContext();
		for (Edge<IntWritable, FWEdgeValueWritable> edge : getEdges()) {
			if (ctx.isTransitive(edge.getValue().getRelation().get())) {
				int j = edge.getTargetVertexId().get();
	//			if (i!=j) {
					i_[j]=1;
					n_[j]=i;
//					n_[j]=edge.getValue().getIdx().get();
	//			}
			}
		}
		i_[i]=0;
		n_[i]=EMPTY;
		setValue(new FWVertexValueWritable(i_,n_));
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
				if (k_[j]!=INFINITY) {
					byte sum = (byte) (i_[k]+k_[j]);
					if (i_[j]==INFINITY) {
						i_[j]=sum;
						n_[j]=pk_[j];
					} else {
						if (sum<i_[j]) {
							i_[j]=sum;
							n_[j]=pk_[j];
						}
					}
				}
			}
			return 0;
		}
		
	}
	
}
