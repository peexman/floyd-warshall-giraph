package net.graph.shortestpath;

import java.io.FileReader;
import java.util.Properties;

import net.graph.shortestpath.floydwarshall.FWMasterCompute;
import net.graph.shortestpath.floydwarshall.FWVertex;
import net.graph.shortestpath.floydwarshall.FWWorkerContext;
import net.graph.shortestpath.floydwarshall.io.formats.FWOutputFormatText;
import net.graph.shortestpath.floydwarshall.io.formats.FWTextEdgeInputFormat;
import net.graph.shortestpath.floydwarshall.io.formats.FWTextVertexInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.partition.SimpleIntRangePartitionerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
		String num_thread = props.getProperty("num_thread");
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
		String partition_count = props.getProperty("partition_count");
		LOG.info("partition_count="+partition_count);
		String compute_threads = props.getProperty("compute_threads");
		LOG.info("compute_threads="+compute_threads);
		Boolean directed = Boolean.parseBoolean(props.getProperty("directed", "true"));
		LOG.info("directed="+directed);
		String dictpath = props.getProperty("dictpath");
		LOG.info("dictpath="+dictpath);
		
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
	    giraphConf.setBoolean("fw.directed", directed);
	    giraphConf.set("fw.dictpath", dictpath);
	    giraphConf.setVertexClass(FWVertex.class);
		giraphConf.setEdgeInputFormatClass(FWTextEdgeInputFormat.class);
//	    giraphConf.setEdgeInputFormatClass(TripleInputFormat.class);
//	    giraphConf.setEdgeInputFormatClass(QuadInputFormat.class);
	    giraphConf.setOutEdgesClass(ByteArrayEdges.class);
		giraphConf.setVertexInputFormatClass(FWTextVertexInputFormat.class);
		giraphConf.setVertexOutputFormatClass(FWOutputFormatText.class);
//		giraphConf.setVertexOutputFormatClass(FWVertexOutputFormat.class);
		giraphConf.setMasterComputeClass(FWMasterCompute.class);
		giraphConf.setWorkerContextClass(FWWorkerContext.class);
		giraphConf.setGraphPartitionerFactoryClass(SimpleIntRangePartitionerFactory.class);
		if (num_thread!=null) giraphConf.setNumComputeThreads(Integer.parseInt(num_thread));
		if (key_space!=null) giraphConf.set(GiraphConstants.PARTITION_VERTEX_KEY_SPACE_SIZE, key_space);
		if (partition_count!=null) giraphConf.set(GiraphConstants.USER_PARTITION_COUNT.getKey(), String.valueOf(partition_count));		
		if (compute_threads!=null) giraphConf.setInt("fw.compute_threads", Integer.parseInt(compute_threads));
		if (zk_list!=null) giraphConf.setZooKeeperConfiguration(zk_list);
		giraphConf.setWorkerConfiguration(min_worker, max_worker, (float)min_worker*100/max_worker);		
        GiraphFileInputFormat.addEdgeInputPath(giraphConf, new Path(in_edges));
        GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(in_vertices));
	    GiraphJob job = new GiraphJob(giraphConf, getClass().getName());
	    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(out_path));

//        Class<?> indexClass = giraphConf.getClass(GiraphConstants.VERTEX_ID_CLASS.getKey(), IntWritable.class);
//        Class<?> valueClass = giraphConf.getClass(GiraphConstants.VERTEX_VALUE_CLASS.getKey(), BytesWritable.class);
        Class<?> indexClass = giraphConf.getClass(GiraphConstants.VERTEX_ID_CLASS.getKey(), Text.class);
        Class<?> valueClass = giraphConf.getClass(GiraphConstants.VERTEX_VALUE_CLASS.getKey(), Text.class);     

	    job.getInternalJob().getConfiguration().setClass("mapred.output.key.class", indexClass, Object.class);
	    job.getInternalJob().getConfiguration().setClass("mapred.output.value.class", valueClass, Object.class);

	    return job.run(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
	    System.exit(ToolRunner.run(new FloydWarshall(), args));
	}

}
