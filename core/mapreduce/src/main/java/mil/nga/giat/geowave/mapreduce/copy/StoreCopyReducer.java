package mil.nga.giat.geowave.mapreduce.copy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;

import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputReducer;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * A basic implementation of copy as a reducer
 */
public class StoreCopyReducer extends
		GeoWaveWritableInputReducer<GeoWaveOutputKey, Object>
{
	private AdapterIndexMappingStore store;

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		store = GeoWaveOutputFormat.getJobContextAdapterIndexMappingStore(context);
	}

	@Override
	protected void reduceNativeValues(
			GeoWaveInputKey key,
			Iterable<Object> values,
			Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		final Iterator<Object> objects = values.iterator();
		while (objects.hasNext()) {
			final AdapterToIndexMapping mapping = store.getIndicesForAdapter(key.getAdapterId());
			context.write(
					new GeoWaveOutputKey<>(
							mapping.getAdapterId(),
							Arrays.asList(mapping.getIndexIds())),
					objects.next());
		}
	}

}
