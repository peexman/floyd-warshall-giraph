package net.graph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Triple;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class NTriple implements Writable
{

	private LongWritable s = new LongWritable();
	private LongWritable p = new LongWritable();
	private LongWritable d = new LongWritable();
	
	public NTriple() {};
	
	public NTriple(String str) throws NTripleException
	{
		try {
			encode(str);
		} catch (NumberFormatException e) {
			throw new NTripleException(e);
		} catch (ParseException e) {
			throw new NTripleException(e);
		}
	}
	
	public LongWritable getSubject() throws NumberFormatException { return s; }
	public LongWritable getPredicate() throws NumberFormatException { return p; }
	public LongWritable getObject() throws NumberFormatException { return d; }
	
	protected void encode(String str) throws NumberFormatException, ParseException
	{
		Node[] nodes = NxParser.parseNodes(str);
		Triple triple = Triple.fromArray(nodes);
		
		encodeSubject(triple);
		encodePredicate(triple);
		encodeObject(triple);
	}
	
	private void encodeSubject(Triple t) throws NumberFormatException {
		String sbj = t.getSubject().toString();
		sbj = sbj.substring(sbj.lastIndexOf('/')+1);
		s.set(new Long(sbj));
	}
	
	private void encodePredicate(Triple t) throws NumberFormatException {
		p.set(new Long(t.getPredicate().toString().hashCode()));
	}
	
	private void encodeObject(Triple t) throws NumberFormatException {
		String obj = t.getObject().toString();
		obj = obj.substring(obj.lastIndexOf('/')+1);
		d.set(new Long(obj));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		s.readFields(in);
		p.readFields(in);
		d.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		s.write(out);
		p.write(out);
		d.write(out);
	}

}
