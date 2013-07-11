package net.graph.io.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class FileLoader<K extends WritableComparable, V extends Writable>
{
	private String infile;
	
	public FileLoader(String infile) { this.infile = infile; }
	
	protected abstract void read(K key, V value);
	
	public void load() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(infile), conf);
		Path path = new Path(infile);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)	ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while (reader.next(key, value)) {
				read((K)key,(V)value);
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
