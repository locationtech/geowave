package mil.nga.giat.geowave.analytic.mapreduce.clustering;

import java.io.IOException;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveWritableInputReducer;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy data from an GeoWave Input to a index using the same adapter.
 * 
 */

public class InputToOutputKeyReducer extends
		GeoWaveWritableInputReducer<GeoWaveOutputKey, Object>
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(InputToOutputKeyReducer.class);

	private GeoWaveOutputKey outputKey;

	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		outputKey.setAdapterId(key.getAdapterId());
		for (final Object value : values) {
			context.write(
					outputKey,
					value);
		}
	}

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		final ConfigurationWrapper config = new JobContextConfigurationWrapper(
				context,
				LOGGER);
		outputKey = new GeoWaveOutputKey(
				new ByteArrayId(
						"na"),
				new ByteArrayId(
						config.getString(
								OutputParameters.Output.INDEX_ID,
								InputToOutputKeyReducer.class,
								"na")));
	}
}
