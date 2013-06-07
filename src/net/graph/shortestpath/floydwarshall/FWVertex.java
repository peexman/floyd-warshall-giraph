package net.graph.shortestpath.floydwarshall;

import java.io.IOException;

import net.graph.adjlist.LongIntAdjList;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

public class FWVertex extends Vertex<LongWritable, MapWritable, NullWritable, NullWritable> 
{
	private static Logger LOG = Logger.getLogger(FWVertex.class);
	
	@Override
	public void compute(Iterable<NullWritable> messages) throws IOException 
	{		
		long V = getTotalNumVertices();		
		long k = getSuperstep()-1;		
		long i = getId().get();
		LongIntAdjList i_;

		if (k<0) {
			i_ = new LongIntAdjList(i);
			for (Edge<LongWritable, NullWritable> edge : getEdges()) {
				long j = edge.getTargetVertexId().get();
				i_.set(j,1);
			}
		}
		else {
			i_ = new LongIntAdjList(i,getValue());
			
			FWWorkerContext ctx = (FWWorkerContext) getWorkerContext();
			LongIntAdjList k_ = new LongIntAdjList(i,ctx.getMap());

			Integer d_ik = i_.get(k);
			for (long j=0; j<V; j++) {
				Integer d_kj = k_.get(j);
				Integer d_ij = i_.get(j);
				if (d_ik!=null && d_kj!=null) {
					int sum = d_ik+d_kj;
					if (d_ij==null)
						i_.set(j,sum);
					else {
						if (sum<d_ij) {
							i_.set(j,sum);
//							i_.setNext(j,k);
						}
					}
				}
			}
		}
		setValue(i_.getMap());

		LOG.debug("(k,i)=("+k+","+i+") i_="+i_.toString(V));
		if (i==getSuperstep()) aggregate(FWAggregator.ID, getValue());
		if (V==getSuperstep()) voteToHalt();		
	}
	
}
