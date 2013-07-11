package net.graph.shortestpath.floydwarshall.io.formats;

import java.io.IOException;

import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

@SuppressWarnings("rawtypes")
public class BinaryInputFormat<K extends WritableComparable, V extends Writable> extends GiraphFileInputFormat<K, V> {

	@Override
	public RecordReader<K, V> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {

		return new SequenceFileRecordReader<K, V>();
	}

}
