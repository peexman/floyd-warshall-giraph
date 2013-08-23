package net.graph.dictionary.mapred;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.regex.Pattern;

import net.graph.io.QuadWritable;
import net.graph.io.TripleWritable;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import sun.awt.SunHints.Value;

public class Tuple2Dictionary 
{
	

	public static class Tuple2DictMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private static Logger LOG = Logger.getLogger(Tuple2DictMapper.class);
		
		private static final Pattern SEPARATOR = Pattern.compile(";");
		
		private long taskId;
		private int c=0;
		
		@Override
		protected void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException 
		{
			long tripleId = taskId | (c++ << 2);
			String line = value.toString().replaceAll("&amp;", "&").replaceAll("&apos;", "'");
		    String[] tokens = SEPARATOR.split(line);
		    for (int i=0; i<tokens.length && i<4; i++) {
		    	StringBuilder sb = new StringBuilder();
		    	if (!tokens[i].isEmpty()) {
		    		if ((tokens.length==4 && (i==1 || i==3)) || (tokens.length==3 && i==2))
		    			sb.append(0);
		    		else if ((tokens.length==4 && i==2) || (tokens.length==3 && i==1))
		    			sb.append(1);
		    		else 
		    			sb.append(2);
		    		sb.append(":").append(tokens[i]);
		    		ctx.write(new Text(sb.toString()), new LongWritable(tripleId | i));
		    	}
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
	
//	public static class TupleKeyComparator extends WritableComparator
//	{
//		private static Logger LOG = Logger.getLogger(TupleKeyComparator.class);
//		
//		protected TupleKeyComparator() {
//			super(Text.class, true);
//		}
//		
//		 @Override
//		 public int compare(WritableComparable w1, WritableComparable w2) 
//		 {
//			 Text t1 = (Text) w1;
//			 Text t2 = (Text) w2;
//			 String[] t1Items = t1.toString().split(":");
//			 String[] t2Items = t2.toString().split(":");
//			 String e1 = t1Items[0] + ":" + t1Items[1];
//			 String e2 = t2Items[0] + ":" + t2Items[1];
//			 
//			 if (t1Items[0].equals("Abdullah Barghouthi") || t2Items[0].equals("Abdullah Barghouthi"))
//				 LOG.info("comparing "+t1.toString()+" to "+t2.toString());
//			 
////			 int comp = t1Items[0].compareTo(t2Items[0]);
//			 int comp = e1.compareTo(e2);
////			 int size1 = Integer.parseInt(t1Items[0]);
////			 int size2 = Integer.parseInt(t2Items[0]);
//			 
//			 if (comp != 0) {
//				 if (t1Items[1].compareTo(t2Items[1]) ==0)
//					 return 0;
////				 if (size1 != 4 && size2 != 4)
////					 comp = size1>size2?1:-1;
//			 } 
////			 else {
////				 comp = size1>size2?-1:1;
////			 }
//			 return comp;
//			 
////			 int size1 = Integer.parseInt(t1Items[1]);
////			 int size2 = Integer.parseInt(t2Items[1]);			 
////
////			 int comp = 0;			 
////			 if (size1 == size2) comp = 0;
////			 else if (size1>size2) comp = -1;
////			 else comp = 1;
////			 
////			 if (comp == 0) {
////				 if (t1Items[0].equals("Abdullah Barghouthi") || t2Items[0].equals("Abdullah Barghouthi"))
////					 LOG.info("stesso size");
////				 if (size1 == 4)
////				 {
////					 Integer pos1 = Integer.parseInt(t1Items[2]);
////					 Integer pos2 = Integer.parseInt(t2Items[2]);
////					 
////					 if ( (pos1 == 1 || pos1 == 3) && (pos2 == 1 || pos2 == 3))
////						 comp = t1Items[0].compareTo(t2Items[0]);
////					 else 
////					 {
////						 if (pos1 == 1 || pos1 == 3) comp = -1;
////						 else if (pos2 == 1 || pos2 == 3) comp = 1;
////						 else comp = t1Items[0].compareTo(t2Items[0]);
////					 }
////				 }
////				 else comp = t1Items[0].compareTo(t2Items[0]);
////			 }
////			 else {
////				 if (t1Items[0].equals("Abdullah Barghouthi") || t2Items[0].equals("Abdullah Barghouthi"))
////					 LOG.info("diverso size");
////				 comp = t1Items[0].compareTo(t2Items[0]);				 
////				 if (comp != 0)
////				 {
////					 if (size1>size2) comp = -1;
////					 else comp = 1;
////				 }
////			 }
////			 if (t1Items[0].equals("Abdullah Barghouthi") || t2Items[0].equals("Abdullah Barghouthi"))
////				 LOG.info("result "+comp);
////			 return comp;
//		 }
//		
//	}
//	
//	public static class TupleGroupComparator extends WritableComparator
//	{
//		private static Logger LOG = Logger.getLogger(TupleGroupComparator.class);
//		
//		protected TupleGroupComparator() {
//			super(Text.class,true);
//		}
//		
//		 @Override
//		 public int compare(WritableComparable w1, WritableComparable w2) 
//		 {
//			 Text t1 = (Text) w1;
//			 Text t2 = (Text) w2;
//			 String[] t1Items = t1.toString().split(":");
//			 String[] t2Items = t2.toString().split(":");
//			 
//			 if (t1Items[1].equals("Abdullah Barghouthi") || t2Items[1].equals("Abdullah Barghouthi"))
//				 LOG.info("grouping "+t1.toString()+" to "+t2.toString()+" res = "+t1Items[1].compareTo(t2Items[1]));
//			 
//			 return t1Items[1].compareTo(t2Items[1]);
//		 }
//		
//	}
//	
//	public static class TuplePartitioner extends Partitioner<Text, LongWritable>  implements Configurable
//	{
//		private static Logger LOG = Logger.getLogger(TuplePartitioner.class);
//		
//		@Override
//		public int getPartition(Text key, LongWritable value, int numPartitions) {
//			String[] t1Items = key.toString().split(":");
//			 if (t1Items[1].equals("Abdullah Barghouthi"))
//				 LOG.info("partitioning "+key.toString()+" to "+t1Items[1].hashCode() % numPartitions+"/"+numPartitions);
//
//			return Math.abs(t1Items[1].hashCode() % numPartitions);
//		}
//
//		@Override
//		public void setConf(Configuration conf) {}
//		
//		@Override
//		public Configuration getConf() { return null; }
//
//	}
	
	public static class Tuple2DictReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
	{
		private static Logger LOG = Logger.getLogger(Tuple2DictReducer.class);
		
//		private OutputStream out;
		private MultipleOutputs mos;
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
			String[] split = key.toString().split(":");
			mos.write("DICT", new Text(split[1]), new Text(String.valueOf(resourceId)));
			if (split[0].equals("0")) {
				mos.write("NODES", new Text(split[1]), new Text(String.valueOf(resourceId)));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,	InterruptedException {
			mos.close();
//			out.close();
		}
		
		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException	
		{
			Configuration conf = ctx.getConfiguration();
			String id = conf.get("mapred.task.id");
			id = id.substring(id.indexOf("_r_")+3).replace("_", "");			
			taskId = Long.parseLong(id) << 32;
			LOG.info("reducer taskId="+taskId);
//			String dictpath = conf.get("dictionary_path");
//			FileSystem fs = FileSystem.get(URI.create(dictpath), conf);
//			out = fs.create(new Path(dictpath));
			mos = new MultipleOutputs(ctx);
		}
	}
	
	public static class Tuple2DictMapper2 extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>
	{
		private static Logger LOG = Logger.getLogger(Tuple2DictMapper2.class);
		
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
		private static Logger LOG = Logger.getLogger(Tuple2DictReducer2.class);
		
		private MultipleOutputs mos;
		private int c=0;
		
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context ctx)
				throws IOException, InterruptedException 
		{
			boolean quad = false;
			int[] ids = new int[4];
			Iterator<LongWritable> it = values.iterator();
			while (it.hasNext()) {
				long value = it.next().get();
				int position = (int) (value & 0x3L);
//				LOG.info("value ricostruito: "+value+"="+Long.toBinaryString(value));
				long encoded = value & ~0x03L;
				long taskId = encoded & ~0xFFFFFFFFL;
				long last32 = (encoded & 0xFFFFFFFFL) >> 2;
				ids[position] = (int)(taskId | last32);
				quad = quad || (position == 3);
			}

			if (quad == true) {
				mos.write("ABOX", NullWritable.get(), new QuadWritable(ids[0],ids[1],ids[2],ids[3],c++));
				mos.write("ABOXt", new Text(String.valueOf(ids[0])), new Text(ids[1]+"\t"+ids[2]+"\t"+ids[3]+"\t"+c));
			} else {
				mos.write("MBOX", NullWritable.get(), new TripleWritable(ids[0],ids[1],ids[2]));
				mos.write("MBOXt", new Text(String.valueOf(ids[0])), new Text(ids[1]+"\t"+ids[2]));
			}
			
		}

		@Override
		protected void cleanup(Context context)	throws IOException, InterruptedException {
			mos.close();
		}

		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			mos = new MultipleOutputs(ctx);
		}
		
		
	}
	
}
