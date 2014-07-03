package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.ingest.GeoWaveData;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is the map-reduce mapper for ingestion with the mapper only.
 */
public class IngestMapper extends
		Mapper<AvroKey, NullWritable, GeoWaveIngestKey, Object>
{
	private IngestWithMapper ingestWithMapper;
	private String globalVisibility;
	private ByteArrayId primaryIndexId;

	@Override
	protected void map(
			final AvroKey key,
			final NullWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		final Iterable<GeoWaveData> data = ingestWithMapper.toGeoWaveData(
				key.datum(),
				primaryIndexId,
				globalVisibility);
		for (final GeoWaveData d : data) {
			context.write(
					d.getKey(),
					d.getValue());
		}
	}

	@Override
	protected void setup(
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			final String ingestWithMapperStr = context.getConfiguration().get(
					AbstractMapReduceIngest.INGEST_PLUGIN_KEY);
			final byte[] ingestWithMapperBytes = ByteArrayUtils.byteArrayFromString(ingestWithMapperStr);
			ingestWithMapper = PersistenceUtils.fromBinary(
					ingestWithMapperBytes,
					IngestWithMapper.class);
			globalVisibility = context.getConfiguration().get(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY);
			final String primaryIndexIdStr = context.getConfiguration().get(
					AbstractMapReduceIngest.PRIMARY_INDEX_ID_KEY);
			if (primaryIndexIdStr != null) {
				primaryIndexId = new ByteArrayId(
						primaryIndexIdStr);
			}
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
