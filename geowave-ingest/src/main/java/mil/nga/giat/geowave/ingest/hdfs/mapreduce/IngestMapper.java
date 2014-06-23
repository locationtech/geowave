package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.store.adapter.AdapterStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class IngestMapper extends
		Mapper<AvroKey, NullWritable, ByteArrayId, Object>
{
	private final static Logger LOGGER = Logger.getLogger(IngestMapper.class);
	public static final String INGEST_WITH_MAPPER_KEY = "INGEST_MAPPER_PLUGIN";
	public static final String GLOBAL_VISIBILITY_KEY = "GLOBAL_VISIBILITY";
	private IngestWithMapper ingestWithMapper;
	private String globalVisibility;
	private AdapterStore adapterStore;

	@Override
	protected void map(
			final AvroKey key,
			final NullWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		final Iterable<GeoWaveData> data = ingestWithMapper.toGeoWaveData(
				key.datum(),
				globalVisibility);
		for (final GeoWaveData d : data) {
			context.write(
					d.getAdapter(
							adapterStore).getAdapterId(),
					d.getData());
		}
	}

	@Override
	protected void setup(
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			adapterStore = GeoWaveOutputFormat.getDataAdapterStore(
					context,
					GeoWaveOutputFormat.getAccumuloOperations(context));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to connect to Accumulo",
					e);
		}

		try {
			final String ingestWithMapperStr = context.getConfiguration().get(
					INGEST_WITH_MAPPER_KEY);
			final byte[] ingestWithMapperBytes = ByteArrayUtils.byteArrayFromString(ingestWithMapperStr);
			ingestWithMapper = PersistenceUtils.fromBinary(
					ingestWithMapperBytes,
					IngestWithMapper.class);
			globalVisibility = context.getConfiguration().get(
					GLOBAL_VISIBILITY_KEY);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
