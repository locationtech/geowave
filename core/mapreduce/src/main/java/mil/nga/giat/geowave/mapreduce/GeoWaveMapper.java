package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class can be extended by GeoWave analytics. It handles the
 * conversion of native GeoWave objects into objects that are writable. It is a
 * mapper that converts to writable objects for both inputs and outputs. This
 * conversion will only work if the data adapter implements HadoopDataAdapter.
 */
public abstract class GeoWaveMapper extends
		Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveWritableInputMapper.class);
	protected HadoopWritableSerializationTool serializationTool;

	@Override
	protected void map(
			final GeoWaveInputKey key,
			final ObjectWritable value,
			final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		mapWritableValue(
				key,
				value,
				context);
	}

	protected void mapWritableValue(
			final GeoWaveInputKey key,
			final ObjectWritable value,
			final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		mapNativeValue(
				key,
				serializationTool.fromWritable(
						key.getAdapterId(),
						value),
				new NativeMapContext<GeoWaveInputKey, ObjectWritable>(
						context,
						serializationTool));
	}

	protected abstract void mapNativeValue(
			final GeoWaveInputKey key,
			final Object value,
			final MapContext<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		serializationTool = new HadoopWritableSerializationTool(
				GeoWaveInputFormat.getJobContextAdapterStore(context));
	}

}
