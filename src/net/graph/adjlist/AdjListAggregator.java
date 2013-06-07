package net.graph.adjlist;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.MapWritable;

public abstract class AdjListAggregator extends BasicAggregator<MapWritable> 
{

	@Override
	public void aggregate(MapWritable row) {
		getAggregatedValue().putAll(row);
	}

	@Override
	public MapWritable createInitialValue() {
		return new MapWritable();
	}

}
