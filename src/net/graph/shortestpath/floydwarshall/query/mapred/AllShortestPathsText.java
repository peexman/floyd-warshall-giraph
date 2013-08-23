package net.graph.shortestpath.floydwarshall.query.mapred;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AllShortestPathsText {

	private static int BUF_LENGTH = 512;
	
	public static class Map extends Mapper<IntWritable, BytesWritable, Text, Text>
	{
		private ByteBuffer path = ByteBuffer.allocate(BUF_LENGTH);
		
		@Override
		protected void map(IntWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			byte[] bytes = value.getBytes();
			int length = value.getLength();
			
			path.clear();
			path.put(bytes, 0, length);
			path.flip();
			
			StringBuffer sb = new StringBuffer();
			while (path.remaining()>0) {
				sb.append(path.getInt()).append(";");
			}
			sb.append("\n");
			
			context.write(new Text(String.valueOf(key.get())), new Text(sb.toString()));
		}
	}
	
//	public static class Map extends Mapper<IntWritable, FWVertexValueWritable, IntWritable, BytesWritable>
//	{
//		private ByteBuffer path = ByteBuffer.allocate(BUF_LENGTH);
//		
//		@Override
//		protected void map(IntWritable key, FWVertexValueWritable value, Context context)
//				throws IOException, InterruptedException {
//			
//			int prv, i = key.get();
//			int[] n_ = value.n().getInts();
//			byte[] i_ = value.i().getBytes();
//			for (int j=0; j<i_.length; j++) {
//				if (i!=j && i_[j]>0) {
//					prv = j;
//					path.clear();
//					while (prv!=i) {
//						path.putInt(prv);
//						prv = n_[prv];
//					}
//					path.flip();
//					byte[] dst = new byte[path.limit()];
//					path.get(dst);
//					
//					context.write(key, new BytesWritable(dst));
//				}
//			}
//		}
//	}
	
//	public static class Reduce extends Reducer<IntWritable, BytesWritable, Text, Text>
//	{
//		private ByteBuffer path = ByteBuffer.allocate(BUF_LENGTH);
//		
//		@Override
//		protected void reduce(IntWritable key, Iterable<BytesWritable> paths, Context context)
//				throws IOException, InterruptedException {
//
//			Iterator<BytesWritable> it = paths.iterator();
//			while (it.hasNext())
//			{
//				BytesWritable bytesWritable = it.next();
//				byte[] bytes = bytesWritable.getBytes();
//				int length = bytesWritable.getLength();
//				
//				path.clear();
//				path.put(bytes, 0, length);
//				path.flip();
//				
//				StringBuffer sb = new StringBuffer();
//				while (path.remaining()>0) {
//					sb.append(path.getInt()).append(";");
//				}
//				sb.append("\n");
//				
//				context.write(new Text(String.valueOf(key.get())), new Text(sb.toString()));
//			}
//		}
//	}

}
