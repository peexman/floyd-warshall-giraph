package net.graph.shortestpath.floydwarshall;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import net.graph.adjlist.LongIntMapWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

public class FWVertex extends Vertex<LongWritable, LongIntMapWritable, NullWritable, NullWritable> 
{
	private static Logger LOG = Logger.getLogger(FWVertex.class);
	private LongIntMapWritable i_, k_;
	private long i, k, V;
	private Integer d_ik;
	
	@Override
	public void compute(Iterable<NullWritable> messages) throws IOException 
	{		
		V = getTotalNumVertices();
		long s = getSuperstep();
		i = getId().get();
		i_ = k_ = null;
		d_ik = null;
		k = s-1;

		if (k<0) setup(); else compute();
		
		setValue(i_);		
		if (i==s) aggregate(FWAggregator.ID, i_);
		if (V==s) voteToHalt();
	}
	
	private void compute() throws IOException
	{
		i_ = getValue();		
		d_ik = i==k?(Integer)0:i_.get(k);
		if (d_ik!=null)	
		{
			FWWorkerContext ctx = (FWWorkerContext) getWorkerContext();
			ExecutorService pool = ctx.getPool();
			k_ = ctx.getMap();		
			
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
		LOG.debug("(k,i)=("+k+","+i+") i_="+i_.toString(i,V));
	}
	
	private ArrayList<Compute> initPool(FWWorkerContext ctx)
	{
		ArrayList<Compute> computeUnits = ctx.getComputeUnits(i);		
		if (computeUnits==null) {
			long start, stop;
			int offset = ctx.getOffset();
//			boolean directed = ctx.isDirected();			
			computeUnits = new ArrayList<Compute>();			
			for (int n=0; n<ctx.getComputeThreads(); n++) {
				start = offset*n;
				stop = start+offset;
//				if (!directed && stop<=i) continue;
//				if (!directed && start<i) start=i;
				if (stop>V) stop=V;
				computeUnits.add(new Compute(start,stop));
			}
			ctx.setComputeUnits(i,computeUnits);
			LOG.info(computeUnits.size()+" compute units initialized for vertex "+i);
		}

		return computeUnits;
	}

	private void setup()
	{
		i_ = new LongIntMapWritable();
		for (Edge<LongWritable, NullWritable> edge : getEdges()) {
			long j = edge.getTargetVertexId().get();
			if (i!=j) i_.set(j,1);
		}
	}
	
	public class Compute implements Callable<Integer>
	{
		private long start;
		private long stop;
		
		public Compute(long start, long stop) {
			this.start = start;
			this.stop = stop;
		}

		@Override
		public Integer call() 
		{
			for (long j=start; j<stop; j++) 
			{
				if (i==j) continue;
				Integer d_kj = k==j?(Integer)0:k_.get(j);
				if (d_kj!=null) {
					Integer d_ij = i==j?(Integer)0:i_.get(j);
					int sum = d_ik+d_kj;
					if (d_ij==null) {
						if (i!=j) i_.set(j,sum);
					} else {
						if (sum<d_ij) {
							i_.set(j,sum);
//							i_.setNext(j,k);
						}
					}
				}
			}
			return 0;
		}
		
	}
	
}
