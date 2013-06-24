package net.graph.old;


import org.apache.giraph.aggregators.BasicAggregator;

public class FWMapAggregator extends BasicAggregator<LongIntMapWritable> 
{
	public static String ID = FWMapAggregator.class.getName();
	
	@Override
	public void aggregate(LongIntMapWritable row) {
		getAggregatedValue().putAll(row);
	}

	@Override
	public LongIntMapWritable createInitialValue() {
		return new LongIntMapWritable();
	}
}
