/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputFormat;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to
 *         <code> GeoWaveWritableOutptMapper </code>
 */
public abstract class GeoWaveHBaseWritableOutputMapper<KEYIN, VALUEIN> extends
		Mapper<KEYIN, VALUEIN, GeoWaveHBaseInputKey, ObjectWritable>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveHBaseWritableOutputMapper.class);
	protected HBaseHadoopWritableSerializationTool serializationTool;

	@Override
	protected void map(
			final KEYIN key,
			final VALUEIN value,
			final Mapper<KEYIN, VALUEIN, GeoWaveHBaseInputKey, ObjectWritable>.Context context )
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
			final Mapper<KEYIN, VALUEIN, GeoWaveHBaseInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		mapNativeValue(
				key,
				value,
				new NativeHBaseMapContext(
						context,
						serializationTool));
	}

	protected abstract void mapNativeValue(
			final KEYIN key,
			final VALUEIN value,
			final MapContext<KEYIN, VALUEIN, GeoWaveHBaseInputKey, Object> context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Mapper<KEYIN, VALUEIN, GeoWaveHBaseInputKey, ObjectWritable>.Context context )
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
