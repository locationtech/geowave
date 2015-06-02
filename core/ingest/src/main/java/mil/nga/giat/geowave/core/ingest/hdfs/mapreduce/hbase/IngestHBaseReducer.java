package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.GeoWaveHBaseData;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.output.GeoWaveHBaseOutputKey;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author viggy Functionality similar to <code> IngestReducer </code>
 */
public class IngestHBaseReducer extends
		Reducer<WritableComparable<?>, Writable, GeoWaveHBaseOutputKey, Object>
{
	private IngestWithReducer ingestWithReducer;
	private String globalVisibility;
	private ByteArrayId primaryIndexId;

	@Override
	protected void reduce(
			final WritableComparable<?> key,
			final Iterable<Writable> values,
			final Context context )
			throws IOException,
			InterruptedException {
		try (CloseableIterator<GeoWaveHBaseData> data = convertDataToHBaseData(ingestWithReducer.toGeoWaveData(
				key,
				primaryIndexId,
				globalVisibility,
				values))) {
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
			final Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			final String ingestWithReducerStr = context.getConfiguration().get(
					AbstractMapReduceHBaseIngest.INGEST_PLUGIN_KEY);
			final byte[] ingestWithReducerBytes = ByteArrayUtils.byteArrayFromString(ingestWithReducerStr);
			ingestWithReducer = PersistenceUtils.fromBinary(
					ingestWithReducerBytes,
					IngestWithReducer.class);
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
