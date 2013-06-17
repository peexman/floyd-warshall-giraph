package net.graph.shortestpath.floydwarshall;

import net.graph.adjlist.LongIntMapWritable;

import org.apache.giraph.aggregators.BasicAggregator;

public class FWAggregator extends BasicAggregator<LongIntMapWritable> 
{
	public static String ID = FWAggregator.class.getName();
	
	@Override
	public void aggregate(LongIntMapWritable row) {
		getAggregatedValue().putAll(row);
	}

	@Override
	public LongIntMapWritable createInitialValue() {
		return new LongIntMapWritable();
	}
}
