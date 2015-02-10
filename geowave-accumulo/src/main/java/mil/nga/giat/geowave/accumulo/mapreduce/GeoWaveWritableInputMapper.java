package mil.nga.giat.geowave.accumulo.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * This abstract class can be extended by GeoWave analytics. It handles the
 * conversion of native GeoWave objects into objects that are writable. It is a
 * mapper that converts to writable objects for the input. This conversion will
 * only work if the data adapter implements HadoopDataAdapter.
 */
public abstract class GeoWaveWritableInputMapper<KEYOUT, VALUEOUT> extends
		Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveWritableInputMapper.class);
	protected HadoopAdapterStore adapterStore;

	@Override
	protected void map(
			final GeoWaveInputKey key,
			final ObjectWritable value,
			final Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		mapWritableValue(
				key,
				value,
				context);
	}

	protected void mapWritableValue(
			final GeoWaveInputKey key,
			final ObjectWritable value,
			final Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		mapNativeValue(
				key,
				adapterStore.fromWritable(
						key.getAdapterId(),
						value),
				context);

	}

	/**
	 * Helper method to create an object writable from a value managed by the
	 * adapter.
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	protected ObjectWritable toWritableValue(
			final GeoWaveInputKey key,
			final Object value ) {
		return adapterStore.toWritable(
				key.getAdapterId(),
				value);
	}

	protected abstract void mapNativeValue(
			final GeoWaveInputKey key,
			final Object value,
			final Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		try {
			adapterStore = new HadoopAdapterStore(
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
