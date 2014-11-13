package mil.nga.giat.geowave.accumulo.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.store.adapter.AdapterStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * This abstract class can be extended by GeoWave analytics. It handles the
 * conversion of native GeoWave objects into objects that are writable. It is a
 * mapper that converts to writable objects for the output. This conversion will
 * only work if the data adapter implements HadoopDataAdapter.
 */
public abstract class GeoWaveWritableOutputMapper<KEYIN, VALUEIN> extends
		Mapper<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveWritableOutputMapper.class);
	protected AdapterStore adapterStore;

	@Override
	protected void map(
			final KEYIN key,
			final VALUEIN value,
			final Mapper<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		mapWritableValue(
				key,
				value,
				context);
	}

	protected void mapWritableValue(
			final KEYIN key,
			final VALUEIN value,
			final Mapper<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		mapNativeValue(
				key,
				value,
				new NativeMapContext(
						context,
						adapterStore));
	}

	protected abstract void mapNativeValue(
			final KEYIN key,
			final VALUEIN value,
			final MapContext<KEYIN, VALUEIN, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Mapper<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		try {
			adapterStore = new JobContextAdapterStore(
					context,
					GeoWaveInputFormat.getAccumuloOperations(context));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to get GeoWave adapter store from job context",
					e);
		}
	}
}
