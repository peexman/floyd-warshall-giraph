package net.graph.shortestpath.floydwarshall;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.master.MasterCompute;

public class FWMasterCompute extends MasterCompute
{
	@Override
	public void readFields(DataInput arg0) throws IOException {}

	@Override
	public void write(DataOutput arg0) throws IOException {}

	@Override
	public void compute() {}

	@Override
	public void initialize() throws InstantiationException,	IllegalAccessException {
		registerAggregator(FWAggregator.ID, FWAggregator.class);
	}

}
