package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.GeoWaveHBaseData;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.output.GeoWaveHBaseOutputKey;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is the map-reduce mapper for ingestion with the mapper only.
 */
public class IngestHBaseMapper extends
		Mapper<AvroKey, NullWritable, GeoWaveHBaseOutputKey, Object>
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
		try (CloseableIterator<GeoWaveHBaseData> data = convertDataToHBaseData(ingestWithMapper.toGeoWaveData(
				key.datum(),
				primaryIndexId,
				globalVisibility))) {
			while (data.hasNext()) {
				final GeoWaveHBaseData d = data.next();
				context.write(
						d.getKey(),
						d.getValue());
			}
		}
	}

	// TODO #406 Need to fix this ugly GeoWaveData to GeoWaveHBaseData
	// conversion
	private CloseableIterator<GeoWaveHBaseData> convertDataToHBaseData(
			final CloseableIterator<GeoWaveData> geoWaveData ) {
		CloseableIterator<GeoWaveHBaseData> data = new CloseableIterator<GeoWaveHBaseData>() {

			@Override
			public boolean hasNext() {
				return geoWaveData.hasNext();
			}

			@Override
			public GeoWaveHBaseData next() {
				GeoWaveData d = geoWaveData.next();
				GeoWaveHBaseData next = new GeoWaveHBaseData(
						d.getAdapterId(),
						d.getIndexId(),
						d.getValue());
				return next;
			}

			@Override
			public void remove() {
				geoWaveData.remove();
			}

			@Override
			public void close()
					throws IOException {
				geoWaveData.close();
			}
		};

		return data;
	}

	@Override
	protected void setup(
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			final String ingestWithMapperStr = context.getConfiguration().get(
					AbstractMapReduceHBaseIngest.INGEST_PLUGIN_KEY);
			final byte[] ingestWithMapperBytes = ByteArrayUtils.byteArrayFromString(ingestWithMapperStr);
			ingestWithMapper = PersistenceUtils.fromBinary(
					ingestWithMapperBytes,
					IngestWithMapper.class);
			globalVisibility = context.getConfiguration().get(
					AbstractMapReduceHBaseIngest.GLOBAL_VISIBILITY_KEY);
			final String primaryIndexIdStr = context.getConfiguration().get(
					AbstractMapReduceHBaseIngest.PRIMARY_INDEX_ID_KEY);
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
