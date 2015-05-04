package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * This abstract class can be extended by GeoWave analytics. It handles the
 * conversion of native GeoWave objects into objects that are writable. It is a
 * reducer that converts to writable objects for the input. This conversion will
 * only work if the data adapter implements HadoopDataAdapter.
 */
public abstract class GeoWaveWritableInputReducer<KEYOUT, VALUEOUT> extends
		Reducer<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveWritableInputReducer.class);
	protected HadoopWritableSerializationTool serializationTool;

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		reduceWritableValues(
				key,
				values,
				context);
	}

	protected void reduceWritableValues(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		final HadoopWritableSerializer<?, Writable> serializer = serializationTool.getHadoopWritableSerializerForAdapter(key.getAdapterId());
		final Iterable<Object> transformedValues = Iterables.transform(
				values,
				new Function<ObjectWritable, Object>() {
					@Override
					public Object apply(
							final ObjectWritable writable ) {
						final Object innerObj = writable.get();
						return (innerObj instanceof Writable) ? serializer.fromWritable((Writable) innerObj) : innerObj;
					}
				});

		reduceNativeValues(
				key,
				transformedValues,
				context);

	}

	protected abstract void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		try {
			serializationTool = new HadoopWritableSerializationTool(
					new JobContextAdapterStore(
							context,
							GeoWaveInputFormat.getAccumuloOperations(context)));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to get GeoWave adapter store from job context",
					e);
		}
	}
}
