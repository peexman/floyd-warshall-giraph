package net.graph.dictionary;

import net.graph.dictionary.mapred.Tuple2Dictionary;
import net.graph.dictionary.mapred.Tuple2Dictionary.Tuple2DictMapper;
import net.graph.dictionary.mapred.Tuple2Dictionary.Tuple2DictMapper2;
import net.graph.dictionary.mapred.Tuple2Dictionary.Tuple2DictReducer;
import net.graph.dictionary.mapred.Tuple2Dictionary.Tuple2DictReducer2;
import net.graph.io.QuadWritable;
import net.graph.io.TripleWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class TupleLoader 
{
	private static Logger LOG = Logger.getLogger(TupleLoader.class);
	
//	private static String in_path;
//	private static String dict_path;
//	private static String out_path;
	
	public static class TupleEncoder extends Configured implements Tool 
	{
		@Override
		public int run(String[] args) throws Exception 
		{		    
			Job job = new Job(getConf());
//			job.getConfiguration().set("dictionary_path", dict_path+"/_dictionary");
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setJarByClass(Tuple2Dictionary.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(Tuple2DictMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
//			job.setSortComparatorClass(TupleKeyComparator.class);
//			job.setGroupingComparatorClass(TupleGroupComparator.class);
//			job.setPartitionerClass(TuplePartitioner.class);
//			job.setNumReduceTasks(1);
			job.setReducerClass(Tuple2DictReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(LongWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
			MultipleOutputs.addNamedOutput(job, "DICT", TextOutputFormat.class, 
					Text.class, Text.class);
			
			MultipleOutputs.addNamedOutput(job, "NODES", TextOutputFormat.class, 
					Text.class, Text.class);			
		    
		    return job.waitForCompletion(true) ? 0 : -1;
		}
	}
	
	public static class TupleDecoder extends Configured implements Tool 
	{
		public static class FileFilter implements PathFilter
		{
			@Override
			public boolean accept(Path path) {
				return !path.getName().startsWith("DICT-r") &&
						!path.getName().startsWith("NODES-r");
			}
		}
		
		@Override
		public int run(String[] args) throws Exception 
		{
			Job job = new Job(getConf());
			
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileInputFormat.setInputPathFilter(job, FileFilter.class);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			job.setJarByClass(Tuple2Dictionary.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapperClass(Tuple2DictMapper2.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setNumReduceTasks(1);
			job.setReducerClass(Tuple2DictReducer2.class);
			
			job.setOutputKeyClass(QuadWritable.class);
			job.setOutputValueClass(NullWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			MultipleOutputs.addNamedOutput(job, "MBOX", SequenceFileOutputFormat.class, 
					NullWritable.class, TripleWritable.class);
			
			MultipleOutputs.addNamedOutput(job, "MBOXt", TextOutputFormat.class, 
					Text.class, Text.class);			

			MultipleOutputs.addNamedOutput(job, "ABOX", SequenceFileOutputFormat.class, 
					NullWritable.class, QuadWritable.class);

			MultipleOutputs.addNamedOutput(job, "ABOXt", TextOutputFormat.class, 
					Text.class, Text.class);
			
		    return job.waitForCompletion(true) ? 0 : -1;
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
//		Properties props = new Properties();
//		if (args!=null && args.length==1)
//			props.load(new FileReader(args[0]));
//		else
//			props.load(new FileReader("tl.properties"));
//
//		in_path = props.getProperty("in_path");
//		LOG.info("in_path="+in_path);
////		dict_path = props.getProperty("dict_path");
////		LOG.info("dict_path="+dict_path);
//		out_path = props.getProperty("out_path");
//		LOG.info("out_path="+out_path);
//		
//		if (in_path==null || out_path==null || dict_path==null) {
//			LOG.error("all of these properties must be set:"+
//					"\n - in_path :"+in_path+
//					"\n - dict_path :"+dict_path+
//					"\n - out_path :"+out_path
//					);
//			return;
//		}
		
		ToolRunner.run(new Configuration(), new TupleEncoder(), args);
		ToolRunner.run(new Configuration(), new TupleDecoder(), args);
	}
}
