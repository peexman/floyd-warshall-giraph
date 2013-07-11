package net.graph.shortestpath.floydwarshall.query;

import net.graph.shortestpath.floydwarshall.query.mapred.AllShortestPathsText;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class GetAllShortestPathsText implements Tool
{
	private static Logger LOG = Logger.getLogger(GetAllShortestPathsText.class);
	
	private static String in_path;
	private static String out_path;
	
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
		Job job = new Job();			
		FileInputFormat.addInputPath(job, new Path(in_path));
		job.setJarByClass(GetAllShortestPathsText.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(AllShortestPathsText.Map.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(AllShortestPathsText.Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(out_path));
	    
	    return job.waitForCompletion(true) ? 0 : -1;
	}

	
	public static void main(String[] args) throws Exception 
	{

//		if (args==null || args.length!=2){
//			LOG.error("all of these properties must be set:"+
//					"\n - in_path :"+
//					"\n - out_path :"
//					);
//			return;
//		}

		in_path = args[0];
		LOG.info("in_path="+in_path);
		out_path = args[1];
		LOG.info("out_path="+out_path);
		
   		ToolRunner.run(new GetAllShortestPathsText(), args);
	}

}
