package net.graph.io.util;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;

public class BerkeleyDB
{	
	private String path;
	private Environment environment;
	private Database primary;
	private SecondaryDatabase secondary;
	private boolean readOnly;

	public BerkeleyDB(String path, boolean readOnly) {
		this.path = path;
		this.readOnly = readOnly;
		this.primary = null;
		this.secondary = null;
		this.environment = null;
	}
	
	public OperationStatus put(Transaction txn, DatabaseEntry key, DatabaseEntry data) {
		return primary.put(txn, key, data);
	}
	
	public OperationStatus put(DatabaseEntry key, DatabaseEntry data) {
		return put(null, key, data);
	}
	
	public OperationStatus get(Transaction txn, DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return primary.get(txn, key, data, lockMode);
	}
	
	public OperationStatus get(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return get(null, key, data, lockMode);
	}
	
	public OperationStatus getSecondary(Transaction txn, DatabaseEntry skey, DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return secondary.get(txn, skey, key, data, lockMode);
	}
	
	public OperationStatus getSecondary(DatabaseEntry skey, DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return getSecondary(null, skey, key, data, lockMode);
	}

	public Cursor openCursor() {
		return primary.openCursor(null,null);
	}
	
	public DiskOrderedCursor openDiskCursor() {
		DiskOrderedCursorConfig cursorConfig = new DiskOrderedCursorConfig();
		return primary.openCursor(cursorConfig);
	}
	
	protected void onOpen(EnvironmentConfig envConfig, DatabaseConfig dbConfig) {}
	
	protected void open(String name) throws IOException
	{
		try {
		    EnvironmentConfig envConfig = new EnvironmentConfig();
		    DatabaseConfig dbConfig = new DatabaseConfig();
		    
		    onOpen(envConfig, dbConfig);
		    
		    envConfig.setAllowCreate(!readOnly);
		    envConfig.setReadOnly(readOnly);		    
		    environment = new Environment(new File(path), envConfig);
		    
		    dbConfig.setAllowCreate(!readOnly);
		    dbConfig.setDeferredWrite(!readOnly);
		    dbConfig.setReadOnly(readOnly);
		    primary = environment.openDatabase(null, name, dbConfig);
		} 
		catch (DatabaseException dbe) {
		    throw new IOException(dbe);
		} 
	}
	
	protected void onOpenSecondary(SecondaryConfig secConfig) {}
	
	protected void openSecondary(String name, SecondaryKeyCreator keyCreator)
	{
		SecondaryConfig secConfig = new SecondaryConfig();
		
		onOpenSecondary(secConfig);
		
		secConfig.setAllowCreate(!readOnly);
		secConfig.setDeferredWrite(!readOnly);
		secConfig.setKeyCreator(keyCreator);
		secConfig.setReadOnly(readOnly);
		
		secondary = environment.openSecondaryDatabase(null, name, primary, secConfig);
	}
	
	public void close() throws IOException
	{
		try {
		    if (secondary != null) {
		    	if (!readOnly) secondary.sync();
		        secondary.close();
		    }
	        if (primary != null) {
	        	if (!readOnly) primary.sync();
	            primary.close();
	        }
		    if (environment != null) {
		    	environment.sync();
		    	if (!readOnly) environment.cleanLog();
		        environment.close();
		    } 
		} catch (DatabaseException dbe) {
			throw new IOException(dbe);
		} 
	}


}
