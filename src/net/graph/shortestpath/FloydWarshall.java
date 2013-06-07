package net.graph.shortestpath;

import java.io.FileReader;
import java.util.Properties;

import net.graph.io.LongVertexInputFormat;
import net.graph.io.NTripleInputFormat;
import net.graph.shortestpath.floydwarshall.FWMasterCompute;
import net.graph.shortestpath.floydwarshall.FWVertex;
import net.graph.shortestpath.floydwarshall.FWVertexOutputFormat;
import net.graph.shortestpath.floydwarshall.FWWorkerContext;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.partition.SimpleLongRangePartitionerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FloydWarshall implements Tool 
{
	private static Logger LOG = Logger.getLogger(FloydWarshall.class);

	private Configuration conf;
	
	@Override
	public Configuration getConf() {
		return conf;
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		Properties props = new Properties();
		if (args!=null && args.length==1)
			props.load(new FileReader(args[0]));
		else
			props.load(new FileReader("fw.properties"));

		String key_space = props.getProperty("key_space");
		LOG.info("key_space="+key_space);
		int num_thread = Integer.parseInt(props.getProperty("num_thread", "5"));
		LOG.info("num_thread="+num_thread);
		String zk_list = props.getProperty("zk_list");
		LOG.info("zk_list="+zk_list);
		int min_worker = Integer.parseInt(props.getProperty("min_worker", "3"));
		LOG.info("min_worker="+min_worker);
		int max_worker = Integer.parseInt(props.getProperty("max_worker", String.valueOf(min_worker)));
		LOG.info("max_worker="+max_worker);
		String in_edges = props.getProperty("in_edges");
		LOG.info("in_edges="+in_edges);
		String in_vertices = props.getProperty("in_vertices");
		LOG.info("in_vertices="+in_vertices);
		String out_path = props.getProperty("out_path");
		LOG.info("out_path="+out_path);
		
		if (in_edges==null || in_vertices==null || out_path==null) {
			LOG.error("all of these properties must be set:"+
					"\n - in_edges :"+in_edges+
					"\n - in_vertices :"+in_vertices+
					"\n - out_path :"+out_path
					);
			return -1;
		}
		
	    if (null == getConf()) { // for YARN profile
	        conf = new Configuration();
	    }
	    
	    GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
	    giraphConf.setVertexClass(FWVertex.class);
		giraphConf.setEdgeInputFormatClass(NTripleInputFormat.class);
		giraphConf.setVertexInputFormatClass(LongVertexInputFormat.class);
		giraphConf.setVertexOutputFormatClass(FWVertexOutputFormat.class);
		giraphConf.setMasterComputeClass(FWMasterCompute.class);
		giraphConf.setWorkerContextClass(FWWorkerContext.class);
		giraphConf.setGraphPartitionerFactoryClass(SimpleLongRangePartitionerFactory.class);
		if (key_space!=null) giraphConf.set(GiraphConstants.PARTITION_VERTEX_KEY_SPACE_SIZE, key_space);
		giraphConf.setNumComputeThreads(num_thread);		
		if (zk_list!=null) giraphConf.setZooKeeperConfiguration(zk_list);
		giraphConf.setWorkerConfiguration(min_worker, max_worker, (float)min_worker*100/max_worker);		
        GiraphFileInputFormat.addEdgeInputPath(giraphConf, new Path(in_edges));
        GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(in_vertices));
	    GiraphJob job = new GiraphJob(giraphConf, getClass().getName());
	    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(out_path));
	    
	    return job.run(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
	    System.exit(ToolRunner.run(new FloydWarshall(), args));
	}

}
