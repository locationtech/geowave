package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is the mapper used when aggregating key value pairs from
 * intermediate data to be ingested into GeoWave using a reducer.
 */
public class IntermediateKeyValueMapper extends
		Mapper<AvroKey, NullWritable, WritableComparable<?>, Writable>
{
	private IngestWithReducer ingestWithReducer;

	@Override
	protected void map(
			final AvroKey key,
			final NullWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		try (CloseableIterator<KeyValueData<WritableComparable<?>, Writable>> data = ingestWithReducer.toIntermediateMapReduceData(key.datum())) {
			while (data.hasNext()) {
				final KeyValueData<WritableComparable<?>, Writable> d = data.next();
				context.write(
						d.getKey(),
						d.getValue());
			}
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
