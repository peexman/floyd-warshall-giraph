package net.graph.shortestpath.floydwarshall.query;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;

import net.graph.dictionary.Dictionary;

import org.apache.log4j.Logger;

public class GetShortestPath 
{
	private static String USAGE = "usage: GetShortestPath <dictionarypath> <dbpath> [<from>] [<to>]";
	private static Logger LOG = Logger.getLogger(GetShortestPath.class);

	private Dictionary dictionary;
	private ShortestPath2BDB paths;
	
	public GetShortestPath(String dictpath, String datapath) throws IOException {
		dictionary = new Dictionary(dictpath,true);
		dictionary.open();
		
		paths = new ShortestPath2BDB(datapath,true);
		paths.open();
	}
	
	public void close() throws IOException {
		if (dictionary!=null) {
			dictionary.close();
			dictionary=null;
		}
		if (paths!=null) {
			paths.close();
			paths=null;
		}
	}
	
	public String printAll() throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		List<int[]> list = paths.getAll();
		Iterator<int[]> it = list.iterator();
		while (it.hasNext()) {
			int[] path = it.next();
			for (int i=path.length-1; i>=0; i--) {
				sb.append(dictionary.get(path[i])).append(";");
//				.append(path[i]).append(")").append(";");
			}
			sb.append("\n");
		}
		sb.append("\n");
		return sb.toString();
	}
	
	public String processQuery(String from, String to) throws UnsupportedEncodingException {
		Integer src = dictionary.get(from);
		Integer dst = dictionary.get(to);
		return processQuery(src, dst);
	}
	
	public String processQuery(String from) throws UnsupportedEncodingException {
		return processQuery(from, null);
	}

	private String processQuery(Integer src, Integer dst) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		List<int[]> list = paths.get(src,dst);
		Iterator<int[]> it = list.iterator();
		while (it.hasNext()) {
			int[] path = it.next();
			for (int i=path.length-1; i>=0; i--) {
				sb.append(dictionary.get(path[i])).append(";");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	public static void main(String[] args) throws IOException {
	
		if (args==null) {
			System.err.println(USAGE);
			System.exit(1);
		}
		
		String result;
		GetShortestPath sp = new GetShortestPath(args[0],args[1]);
		switch (args.length)
		{
		case 2:
			result = sp.printAll();
			break;
		case 3:
			result = sp.processQuery(args[2]);
			break;
		case 4:
			result = sp.processQuery(args[2],args[3]);
			break;
		default:
			result = USAGE;
		}
		sp.close();

		System.out.println(result);
	}


}
