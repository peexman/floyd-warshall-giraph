package net.graph.shortestpath.floydwarshall;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.MapWritable;

public class FWWorkerContext extends WorkerContext
{
	private MapWritable map = null;
	
	public MapWritable getMap() { return map; }

	@Override
	public void preSuperstep() {
		map = getAggregatedValue(FWAggregator.ID);
	}

	@Override
	public void postSuperstep()	{}
	
	@Override
	public void preApplication() throws InstantiationException,	IllegalAccessException {}
	
	@Override
	public void postApplication() {}


}
