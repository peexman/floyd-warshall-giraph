package net.graph.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import net.graph.io.util.BerkeleyDB;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class Dictionary extends BerkeleyDB
{	
	private static final String dbname = Dictionary.class.getName();
	private static final String db2name = dbname+"_sec";
	
	private final EntryBinding<Integer> intBinding = TupleBinding.getPrimitiveBinding(Integer.class);
	
	public Dictionary(String path, boolean readOnly) {
		super(path,readOnly);
	}
	
	@Override
	protected void onOpenSecondary(SecondaryConfig secConfig) {
		secConfig.setAllowPopulate(true);
	}

	public void open() throws IOException {
		open(dbname);
		openSecondary(db2name, new DictionarySecondaryKeyCreator());
	}
	
	public List<String> getKeys() throws UnsupportedEncodingException 
	{
		List<String> res = new ArrayList<String>();
		Cursor cursor = null;
		try
		{
			cursor = openCursor();
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();
		    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
		    	res.add(new String(foundKey.getData(), "UTF-8"));
		    }
		}
		finally {
			cursor.close();
		}
	    return res;
	}
	
	public List<Integer> getValues() throws UnsupportedEncodingException 
	{
		List<Integer> res = new ArrayList<Integer>();
		Cursor cursor = null;
		try
		{
			cursor = openCursor();
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();
		    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
		    	res.add(intBinding.entryToObject(foundData));
		    }
		}
		finally {
			cursor.close();
		}
	    return res;
	}
	
	public Integer get(String key) throws UnsupportedEncodingException {
		if (key==null) return null;
		DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
	    DatabaseEntry theData = new DatabaseEntry();
	    if (get(theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	        return intBinding.entryToObject(theData);
	    }
	    return null; 
	}

	
	public String get(Integer id) throws UnsupportedEncodingException {
		DatabaseEntry bkey = new DatabaseEntry();
		intBinding.objectToEntry(id, bkey);
		DatabaseEntry theKey = new DatabaseEntry();
	    DatabaseEntry theData = new DatabaseEntry();
	    if (getSecondary(bkey, theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	    	byte[] retData = theKey.getData();
	        return new String(retData, "UTF-8");
	    }
	    return null; 
	}
	
	public static class DictionarySecondaryKeyCreator implements SecondaryKeyCreator 
	{
		@Override
		public boolean createSecondaryKey(SecondaryDatabase db,
				DatabaseEntry key, DatabaseEntry value, DatabaseEntry resKey) {

			resKey.setData(value.getData());
			return true;
		}
		
	}
	
	public static void main(String[] args) throws IOException 
	{
		if (args==null || args.length!=1) {
			System.err.println("usage: Dictionary <out_db_path> < <dictionary_path>");
		}
		
		final Pattern SEPARATOR = Pattern.compile("\t");
		final EntryBinding<Integer> binding = TupleBinding.getPrimitiveBinding(Integer.class);
		final Dictionary dictionary = new Dictionary(args[0], false);

		try
		{
			String input;
			dictionary.open();			
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			while((input=br.readLine())!=null) {
				String[] tokens = SEPARATOR.split(input);
				DatabaseEntry bvalue = new DatabaseEntry();
				binding.objectToEntry(Integer.parseInt(tokens[1]), bvalue);
			    dictionary.put(new DatabaseEntry(tokens[0].getBytes("UTF-8")), bvalue);
			}
			dictionary.close();	 
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch(IOException io){
			io.printStackTrace();
		}

		System.out.println("done.");
	}

}
