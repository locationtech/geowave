package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class IntermediateMapper extends
		Mapper<AvroKey, NullWritable, Writable, Writable>
{
	private IngestWithReducer ingestWithReducer;

	@Override
	protected void map(
			final AvroKey key,
			final NullWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		final Iterable<IntermediateData<Writable, Writable>> data = ingestWithReducer.toIntermediateMapReduceData(key.datum());
		for (final IntermediateData<Writable, Writable> d : data) {
			context.write(
					d.getKey(),
					d.getValue());
		}
	}

	@Override
	protected void setup(
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			final String ingestWithReducerStr = context.getConfiguration().get(
					AbstractMapReduceIngest.INGEST_PLUGIN_KEY);
			final byte[] ingestWithReducerBytes = ByteArrayUtils.byteArrayFromString(ingestWithReducerStr);
			ingestWithReducer = PersistenceUtils.fromBinary(
					ingestWithReducerBytes,
					IngestWithReducer.class);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
