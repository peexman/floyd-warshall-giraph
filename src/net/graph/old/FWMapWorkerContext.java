package net.graph.old;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.graph.old.FWMapVertex.Compute;

import org.apache.giraph.worker.WorkerContext;
import org.apache.log4j.Logger;

public class FWMapWorkerContext extends WorkerContext
{
	private static Logger LOG = Logger.getLogger(FWMapWorkerContext.class);
	
	private Map<Long,ArrayList<Compute>> compute_units = new HashMap<Long,ArrayList<Compute>>();
	private boolean directed = false;
	private LongIntMapWritable map;
	private ExecutorService pool;
	private int offset, nthreads;
	
	public ArrayList<Compute> getComputeUnits(Long i) { return compute_units.get(i); }
	public void setComputeUnits(Long i, ArrayList<Compute> computeUnits) { compute_units.put(i, computeUnits); }
	public boolean isDirected() { return directed; }
	public LongIntMapWritable getMap() { return map; }
	public ExecutorService getPool() { return pool; }
	public int getComputeThreads() { return nthreads; }
	public int getOffset() { return offset; }

	@Override
	public void preSuperstep() {
		map = getAggregatedValue(FWMapAggregator.ID);
		
		if (getSuperstep()==1) {
			directed = getContext().getConfiguration().getBoolean("fw.directed", false);
			int r = getContext().getConfiguration().getInt("giraph.numComputeThreads", 1);
			nthreads = getContext().getConfiguration().getInt("fw.compute_threads", r*2);
			offset = (int) Math.ceil((double)getTotalNumVertices()/nthreads);
		}
	}

	@Override
	public void postSuperstep()	{}
	
	@Override
	public void preApplication() throws InstantiationException,	IllegalAccessException {
		pool = Executors.newCachedThreadPool();		
	}
	
	@Override
	public void postApplication() 
	{		
		try {
			pool.shutdown();
			while (!pool.isTerminated())			
				pool.awaitTermination(100, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			LOG.warn("Problem shutting down thread pool", e);
		} finally {
			pool = null;
		}
	}

}
