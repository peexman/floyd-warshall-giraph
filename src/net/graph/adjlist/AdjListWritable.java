package net.graph.adjlist;

import org.apache.hadoop.io.BytesWritable;

public class AdjListWritable extends BytesWritable
{
	public static final byte INFINITY = Byte.MIN_VALUE;
	
	public AdjListWritable() {}
	
	public AdjListWritable(byte[] bytes) {
		super(bytes);
	}
	
	public int sum()
	{
		int sum=0;
		byte[] bytes = getBytes();
		for (int j=0; j<bytes.length; j++)
			sum += bytes[j]==INFINITY?0:bytes[j];
		return sum;
	}
	
	public Float avg() 
	{
		byte[] bytes = getBytes();
		if (bytes.length==0) return null;
		return (float)sum()/(bytes.length*(bytes.length-1));
	}
	
	@Override
	public String toString()
	{		
		StringBuilder sb = new StringBuilder();
		byte[] bytes = getBytes();
		for (int j=0; j<bytes.length; j++) {
			byte b = bytes[j]; 
			sb.append(b==INFINITY?'-':b).append('\t');
		}
		return sb.toString();
	}

}
