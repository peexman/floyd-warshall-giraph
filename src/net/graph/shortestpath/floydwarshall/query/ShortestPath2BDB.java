package net.graph.shortestpath.floydwarshall.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import net.graph.io.util.BerkeleyDB;
import net.graph.io.util.FileLoader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class ShortestPath2BDB extends BerkeleyDB
{
	private static final String dbname = ShortestPath2BDB.class.getName();
	private final EntryBinding<Integer> intBinding = TupleBinding.getPrimitiveBinding(Integer.class);
	
	private static int BUF_LENGTH = 512;
	private ByteBuffer buf = ByteBuffer.allocate(BUF_LENGTH);
	
	public ShortestPath2BDB(String path, boolean readOnly) {
		super(path, readOnly);
	}
		
	@Override
	protected void onOpen(EnvironmentConfig envConfig, DatabaseConfig dbConfig) {
		dbConfig.setSortedDuplicates(true);
	}

	public void open() throws IOException {
		open(dbname);
	}
	
	public List<int[]> getAll()
	{
		DiskOrderedCursor cursor = null;
		List<int[]> res = new ArrayList<int[]>();		
		try
		{
			cursor = openDiskCursor();
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();
			OperationStatus retVal = cursor.getNext(foundKey, foundData, null);
			while (retVal == OperationStatus.SUCCESS) {
	        	int k=intBinding.entryToObject(foundKey);
				
				buf.clear();
	        	buf.put(foundData.getData());
	        	buf.putInt(k);
	        	buf.flip();
	        	
	        	IntBuffer asIntBuffer = buf.asIntBuffer();
	        	int data[] = new int[asIntBuffer.limit()];
	        	asIntBuffer.get(data);
	        	res.add(data);

	        	retVal = cursor.getNext(foundKey, foundData, null);
			}
		}
		finally {
			cursor.close();
		}
		return res;
	}

	public List<int[]> get(Integer src, Integer dst) 
	{
		Cursor cursor = null;
		List<int[]> res = new ArrayList<int[]>();
		try
		{
			cursor = openCursor();
			DatabaseEntry key = new DatabaseEntry();	    
		    intBinding.objectToEntry(src, key);
		    DatabaseEntry theData = new DatabaseEntry();
		    OperationStatus retVal = cursor.getSearchKey(key, theData, LockMode.DEFAULT);
	        while (retVal == OperationStatus.SUCCESS)
	        {
				buf.clear();
	        	buf.put(theData.getData());
	        	buf.putInt(src);
	        	buf.flip();
	        	
	        	IntBuffer asIntBuffer = buf.asIntBuffer();
	        	int data[] = new int[asIntBuffer.limit()];
	        	asIntBuffer.get(data);
	        	
	        	if (dst!=null) {
	        		if (data[0]==dst) 
	        			res.add(data);
	        	}
	        	else
	        		res.add(data);

	            retVal = cursor.getNextDup(key, theData, LockMode.DEFAULT);
	        }
		}
		finally {
			cursor.close();
		}
		return res;
	}


	public static void main(String[] args) throws IOException {
		
		if (args==null || args.length!=2) {
			System.err.println("usage: GetShortestPath2BDB <dboutpath> <inpathsfile>");
		}
		
		final EntryBinding<Integer> myBinding = TupleBinding.getPrimitiveBinding(Integer.class);
		final ShortestPath2BDB db = new ShortestPath2BDB(args[0],false);
		
		db.open();
		new FileLoader<IntWritable,BytesWritable>(args[1]) {

			@Override
			protected void read(IntWritable key, BytesWritable value) {
				DatabaseEntry bkey = new DatabaseEntry();
			    myBinding.objectToEntry(key.get(), bkey);
			    
				byte[] bytes = value.getBytes();
				int length = value.getLength();
				
				db.put(bkey, new DatabaseEntry(bytes,0,length));
			}
			
		}.load();
		db.close();

		System.out.println("done.");
	}

}
