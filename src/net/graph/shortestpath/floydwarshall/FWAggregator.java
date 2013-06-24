package net.graph.shortestpath.floydwarshall;

import net.graph.adjlist.AdjListWritable;

import org.apache.giraph.aggregators.BasicAggregator;

public class FWAggregator extends BasicAggregator<AdjListWritable> 
{
	public static String ID = FWAggregator.class.getName();

	@Override
	public void aggregate(AdjListWritable newData) {
		if (newData.getBytes().length>0) {
			getAggregatedValue().set(newData);
		}
	}

	@Override
	public AdjListWritable createInitialValue() {
		return new AdjListWritable();
	}

}
