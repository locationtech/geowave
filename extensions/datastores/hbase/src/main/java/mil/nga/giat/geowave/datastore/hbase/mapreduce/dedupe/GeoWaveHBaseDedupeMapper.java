/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce.dedupe;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.hbase.mapreduce.GeoWaveHBaseWritableOutputMapper;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputKey;

import org.apache.hadoop.mapreduce.MapContext;

/**
 * @author viggy Functionality similar to <code> GeoWaveDedupeMapper </code>
 */
public class GeoWaveHBaseDedupeMapper extends
		GeoWaveHBaseWritableOutputMapper<GeoWaveHBaseInputKey, Object>
{

	@Override
	protected void mapNativeValue(
			GeoWaveHBaseInputKey key,
			Object value,
			MapContext<GeoWaveHBaseInputKey, Object, GeoWaveHBaseInputKey, Object> context )
			throws IOException,
			InterruptedException {
		context.write(
				key,
				value);
	}

}
