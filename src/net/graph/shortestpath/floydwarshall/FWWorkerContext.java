package net.graph.shortestpath.floydwarshall;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.graph.dictionary.Dictionary;
import net.graph.shortestpath.floydwarshall.FWVertex.Compute;
import net.graph.shortestpath.floydwarshall.io.FWVertexValueWritable;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.metrics.file.FileContext;
import org.apache.log4j.Logger;

import com.sleepycat.je.ThreadInterruptedException;

public class FWWorkerContext extends WorkerContext
{
	private static Logger LOG = Logger.getLogger(FWWorkerContext.class);
	
	private Map<Integer,ArrayList<Compute>> compute_units = new HashMap<Integer,ArrayList<Compute>>();
	private boolean directed = false;
	private FWVertexValueWritable previous;
	private ExecutorService pool;
	private int offset, nthreads;
//	private Dictionary dictionary;
	private List<String> dictionary = new ArrayList<String>();
	
	public ArrayList<Compute> getComputeUnits(Integer i) { return compute_units.get(i); }
	public void setComputeUnits(Integer i, ArrayList<Compute> computeUnits) { compute_units.put(i, computeUnits); }
	public boolean isDirected() { return directed; }
	public FWVertexValueWritable getPrevious() { return previous; }
	public ExecutorService getPool() { return pool; }
	public int getComputeThreads() { return nthreads; }
	public int getOffset() { return offset; }
	
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		}

	@Override
	public void preSuperstep() {
		previous = getAggregatedValue(FWAggregator.ID);
	}

	@Override
	public void postSuperstep()	{}
	
	@Override
	public void preApplication() throws InstantiationException,	IllegalAccessException 
	{
		directed = getContext().getConfiguration().getBoolean("fw.directed", false);
		int processors = Runtime.getRuntime().availableProcessors();
		LOG.info("detected "+processors+" available core(s)");
		nthreads = getContext().getConfiguration().getInt("fw.compute_threads", processors*2);
		LOG.info("setting fw.compute_threads to "+nthreads);
		offset = (int) Math.ceil((double)getTotalNumVertices()/nthreads);
		pool = Executors.newFixedThreadPool(nthreads);
		
		String dictpath = getContext().getConfiguration().get("fw.dictpath");
		InputStream in = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
		try {
			in = new URL("hdfs://"+dictpath).openStream();
			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			
			String line = "";
			while ((line=br.readLine()) != null) {
				String[] split = line.split("\t");
				dictionary.add(split[0]);
			}

		} catch (Throwable e) {
			LOG.error("error opening dictionary from path "+dictpath, e);
			throw new InstantiationException(e.getLocalizedMessage());
		} 
		finally {
			IOUtils.closeStream(br);
			IOUtils.closeStream(isr);
			IOUtils.closeStream(in);			
		}
		
//		dictionary = new Dictionary(dictpath,true);
//		try {
//			dictionary.open();
//		} catch (IOException e) {
//			LOG.error("error opening dictionary from path "+dictpath, e);
//			throw new InstantiationException(e.getLocalizedMessage());			
//		}
	}
	
	@Override
	public void postApplication() 
	{		
		try {
//			dictionary.close();

			pool.shutdown();
			while (!pool.isTerminated())			
				pool.awaitTermination(100, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			LOG.warn("Problem shutting down thread pool", e);
		} 
//		catch (IOException e) {
//			LOG.warn("Problem closing dictionary", e);
//		} 
		finally {
			pool = null;
//			dictionary = null;
		}
	}
	public boolean isTransitive(int rel_id) {
		String relation = "";
//		try 
//		{
//			relation = dictionary.get(rel_id);
//		} 
//		catch (UnsupportedEncodingException e) {
//			LOG.error("error accessing dictionary", e);
//		}
		relation = dictionary.get(rel_id);
		return relation.equals("COMMUNICATE");
	}
	
	public int getIDFromDictionary(String entry) {
		for (int i=0; i<dictionary.size(); i++) {
			if (dictionary.get(i).equals(entry))
				return i;
		}
		return -1;
	}

}
