package net.graph.dictionary.mapred;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.regex.Pattern;

import net.graph.io.TripleWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class Tuple2Dictionary 
{
	private static Logger LOG = Logger.getLogger(Tuple2Dictionary.class);

	public static class Tuple2DictMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private static final Pattern SEPARATOR = Pattern.compile("\t");
		
		private long taskId;
		private int c=0;
		
		@Override
		protected void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException 
		{
			long tripleId = taskId | (c++ << 2);
		    String[] tokens = SEPARATOR.split(value.toString());
		    for (int i=0; i<tokens.length && i<3; i++) {
		    	ctx.write(new Text(tokens[i]), new LongWritable(tripleId | i));
		    }
		}
		
		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			String id = ctx.getConfiguration().get("mapred.task.id");
			id = id.substring(id.indexOf("_m_")+3).replace("_", "");
			taskId = Long.parseLong(id) << 32;
			LOG.info("mapper taskId="+taskId);
		}
	}
	
	public static class Tuple2DictReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
	{
		private OutputStream out;
		private long taskId;
		private int c=0;

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context ctx)
				throws IOException, InterruptedException 
		{
			long resourceId = taskId | c++;
			Iterator<LongWritable> it = values.iterator();
			while (it.hasNext()) {				
				ctx.write(new LongWritable(resourceId), it.next());
			}
			StringBuilder sb = new StringBuilder();
			sb.append(key).append("\t").append(resourceId).append("\n");
			out.write(sb.toString().getBytes("UTF-8"));
		}

		@Override
		protected void cleanup(Context context) throws IOException,	InterruptedException {
			super.cleanup(context);
			out.close();
		}

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException	
		{
			Configuration conf = ctx.getConfiguration();
			String id = conf.get("mapred.task.id");
			id = id.substring(id.indexOf("_r_")+3).replace("_", "");			
			taskId = Long.parseLong(id) << 32;
			LOG.info("reducer taskId="+taskId);
			String dictpath = conf.get("dictionary_path");
			FileSystem fs = FileSystem.get(URI.create(dictpath), conf);
			out = fs.create(new Path(dictpath));
		}
	}
	
	public static class Tuple2DictMapper2 extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>
	{
		@Override
		protected void map(LongWritable key, LongWritable value, Context ctx)
				throws IOException, InterruptedException 
		{
			long tripleId = value.get();
			long position = tripleId & 0x03L;
			long resourceId = key.get();
			long taskId = resourceId & ~0xFFFFFFFFL;
			long last32 = (resourceId & 0xFFFFFFFFL) << 2;
//			LOG.info((taskId | last32 | position)+"="+Long.toBinaryString(taskId | last32 | position));
			ctx.write(new LongWritable(tripleId & ~0x03L), new LongWritable(taskId | last32 | position));
		}
	}
	
	public static class Tuple2DictReducer2 extends Reducer<LongWritable, LongWritable, NullWritable, TripleWritable>
	{
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context ctx)
				throws IOException, InterruptedException 
		{
			int[] ids = new int[3];
			Iterator<LongWritable> it = values.iterator();
			while (it.hasNext()) {
				long value = it.next().get();
				int position = (int) (value & 0x3L);
//				LOG.info(value+"="+Long.toBinaryString(value));
				long encoded = value & ~0x03L;
				long taskId = encoded & ~0xFFFFFFFFL;
				long last32 = (encoded & 0xFFFFFFFFL) >> 2;
				ids[position] = (int)(taskId | last32);
			}
			ctx.write(NullWritable.get(), new TripleWritable(ids[0],ids[1],ids[2]));//Text(ids[0]+" "+ids[1]+" "+ids[2]));
		}
	}

}
