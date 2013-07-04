package net.graph.shortestpath.floydwarshall;

import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.BytesWritable;

public class FWAggregator extends BasicAggregator<FWVertexValueWritable> 
{
	public static String ID = FWAggregator.class.getName();

	@Override
	public void aggregate(FWVertexValueWritable newData) {
		getAggregatedValue().aggregate(newData);
	}

	@Override
	public FWVertexValueWritable createInitialValue() {
		return new FWVertexValueWritable();
	}

}
