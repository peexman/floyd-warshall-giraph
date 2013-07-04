package net.graph.shortestpath.floydwarshall;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.graph.shortestpath.floydwarshall.FWVertex.Compute;
import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class FWWorkerContext extends WorkerContext
{
	private static Logger LOG = Logger.getLogger(FWWorkerContext.class);
	
	private Map<Integer,ArrayList<Compute>> compute_units = new HashMap<Integer,ArrayList<Compute>>();
	private boolean directed = false;
	private FWVertexValueWritable previous;
	private ExecutorService pool;
	private int offset, nthreads;
	
	public ArrayList<Compute> getComputeUnits(Integer i) { return compute_units.get(i); }
	public void setComputeUnits(Integer i, ArrayList<Compute> computeUnits) { compute_units.put(i, computeUnits); }
	public boolean isDirected() { return directed; }
	public FWVertexValueWritable getPrevious() { return previous; }
	public ExecutorService getPool() { return pool; }
	public int getComputeThreads() { return nthreads; }
	public int getOffset() { return offset; }

	@Override
	public void preSuperstep() {
		previous = getAggregatedValue(FWAggregator.ID);
	}

	@Override
	public void postSuperstep()	{}
	
	@Override
	public void preApplication() throws InstantiationException,	IllegalAccessException 
	{
		directed = getContext().getConfiguration().getBoolean("fw.directed", false);
		int processors = Runtime.getRuntime().availableProcessors();
		LOG.info("detected "+processors+" available core(s)");
		nthreads = getContext().getConfiguration().getInt("fw.compute_threads", processors*2);
		LOG.info("setting fw.compute_threads to "+nthreads);
		offset = (int) Math.ceil((double)getTotalNumVertices()/nthreads);
		pool = Executors.newFixedThreadPool(nthreads);		
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
