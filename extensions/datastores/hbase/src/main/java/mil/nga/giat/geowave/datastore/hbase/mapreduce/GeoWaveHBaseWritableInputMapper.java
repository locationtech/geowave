/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputFormat;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to
 *         <code> GeoWaveWritableInputMapper </code>
 */
public abstract class GeoWaveHBaseWritableInputMapper<KEYOUT, VALUEOUT> extends
		Mapper<GeoWaveHBaseInputKey, ObjectWritable, KEYOUT, VALUEOUT>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveHBaseWritableInputMapper.class);
	protected HBaseHadoopWritableSerializationTool serializationTool;

	@Override
	protected void map(
			final GeoWaveHBaseInputKey key,
			final ObjectWritable value,
			final Mapper<GeoWaveHBaseInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		mapWritableValue(
				key,
				value,
				context);
	}

	protected void mapWritableValue(
			final GeoWaveHBaseInputKey key,
			final ObjectWritable value,
			final Mapper<GeoWaveHBaseInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		mapNativeValue(
				key,
				serializationTool.fromWritable(
						key.getAdapterId(),
						value),
				context);

	}

	protected ObjectWritable toWritableValue(
			final GeoWaveHBaseInputKey key,
			final Object value ) {
		return serializationTool.toWritable(
				key.getAdapterId(),
				value);
	}

	protected abstract void mapNativeValue(
			final GeoWaveHBaseInputKey key,
			final Object value,
			final Mapper<GeoWaveHBaseInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Mapper<GeoWaveHBaseInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		try {
			serializationTool = new HBaseHadoopWritableSerializationTool(
					new JobContextHBaseAdapterStore(
							context,
							GeoWaveHBaseInputFormat.getOperations(context)));
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to get GeoWave adapter store from job context",
					e);
		}
	}
}
