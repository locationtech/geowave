package mil.nga.giat.geowave.mapreduce.copy;

import java.io.IOException;

import org.apache.hadoop.mapreduce.MapContext;

import mil.nga.giat.geowave.mapreduce.GeoWaveWritableOutputMapper;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

/**
 * Basically an identity mapper used for the copy job
 */
public class StoreCopyMapper extends
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
