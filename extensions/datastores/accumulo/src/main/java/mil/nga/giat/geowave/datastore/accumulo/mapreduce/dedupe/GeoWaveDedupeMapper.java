package mil.nga.giat.geowave.datastore.accumulo.mapreduce.dedupe;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveWritableOutputMapper;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.mapreduce.MapContext;

/**
 * Basically an identity mapper used for the deduplication job
 */
public class GeoWaveDedupeMapper extends
		GeoWaveWritableOutputMapper<GeoWaveInputKey, Object>
{

	@Override
	protected void mapNativeValue(
			final GeoWaveInputKey key,
			final Object value,
			final MapContext<GeoWaveInputKey, Object, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException {
		context.write(
				key,
				value);
	}

}
