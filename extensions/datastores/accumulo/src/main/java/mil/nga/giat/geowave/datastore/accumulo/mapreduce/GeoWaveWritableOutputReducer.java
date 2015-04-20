package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * This abstract class can be extended by GeoWave analytics. It handles the
 * conversion of native GeoWave objects into objects that are writable. It is a
 * reducer that converts to writable objects for the output. This conversion
 * will only work if the data adapter implements HadoopDataAdapter.
 */
public abstract class GeoWaveWritableOutputReducer<KEYIN, VALUEIN> extends
		Reducer<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveWritableOutputReducer.class);
	protected HadoopWritableSerializationTool serializationTool;

	@Override
	protected void reduce(
			final KEYIN key,
			final Iterable<VALUEIN> values,
			final Reducer<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		reduceWritableValues(
				key,
				values,
				context);
	}

	protected void reduceWritableValues(
			final KEYIN key,
			final Iterable<VALUEIN> values,
			final Reducer<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		reduceNativeValues(
				key,
				values,
				new NativeReduceContext(
						context,
						serializationTool));
	}

	protected abstract void reduceNativeValues(
			final KEYIN key,
			final Iterable<VALUEIN> values,
			final ReduceContext<KEYIN, VALUEIN, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Reducer<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable>.Context context )
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
