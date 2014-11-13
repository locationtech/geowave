package mil.nga.giat.geowave.accumulo.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * This abstract class can be extended by GeoWave analytics. It handles the
 * conversion of native GeoWave objects into objects that are writable.It is a
 * reducer that converts to writable objects for both inputs and outputs. This
 * conversion will only work if the data adapter implements HadoopDataAdapter.
 */
public abstract class GeoWaveReducer extends
		Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveReducer.class);
	protected AdapterStore adapterStore;

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
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
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		if (adapterStore != null) {
			final DataAdapter<?> adapter = adapterStore.getAdapter(key.getAdapterId());
			if ((adapter != null) && (adapter instanceof HadoopDataAdapter)) {
				final Iterable<Object> transformedValues = Iterables.transform(
						values,
						new Function<ObjectWritable, Object>() {
							@Override
							public Object apply(
									final ObjectWritable writable ) {
								return ((HadoopDataAdapter) adapter).fromWritable((Writable) writable.get());
							}
						});
				reduceNativeValues(
						key,
						transformedValues,
						new NativeReduceContext(
								context,
								adapterStore));
			}
		}
	}

	protected abstract void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final ReduceContext<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
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
