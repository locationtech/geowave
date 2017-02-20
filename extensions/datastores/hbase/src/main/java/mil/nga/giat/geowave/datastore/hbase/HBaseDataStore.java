/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.HBaseSplitsProvider;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;

public class HBaseDataStore extends
		BaseMapReduceDataStore
{
	private final static Logger LOGGER = Logger.getLogger(
			HBaseDataStore.class);

	private final HBaseOperations operations;
	private final HBaseOptions options;

	private final HBaseSplitsProvider splitsProvider = new HBaseSplitsProvider();

	public HBaseDataStore(
			final HBaseOperations operations,
			final HBaseOptions options ) {
		this(
				new IndexStoreImpl(
						operations,
						options),
				new AdapterStoreImpl(
						operations,
						options),
				new DataStatisticsStoreImpl(
						operations,
						options),
				new AdapterIndexMappingStoreImpl(
						operations,
						options),
				new HBaseSecondaryIndexDataStore(
						operations,
						options),
				operations,
				options);
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final HBaseSecondaryIndexDataStore secondaryIndexDataStore,
			final HBaseOperations operations,
			final HBaseOptions options ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options);

		this.operations = operations;
		this.options = options;
		secondaryIndexDataStore.setDataStore(
				this);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {}

	protected boolean rowHasData(
			final byte[] rowId,
			final List<ByteArrayId> dataIds )
			throws IOException {

		final byte[] metadata = Arrays.copyOfRange(
				rowId,
				rowId.length - 12,
				rowId.length);

		final ByteBuffer metadataBuf = ByteBuffer.wrap(
				metadata);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				rowId,
				0,
				rowId.length - 12);
		final byte[] indexId = new byte[rowId.length - 12 - adapterIdLength - dataIdLength];
		final byte[] rawAdapterId = new byte[adapterIdLength];
		final byte[] rawDataId = new byte[dataIdLength];
		buf.get(
				indexId);
		buf.get(
				rawAdapterId);
		buf.get(
				rawDataId);

		for (final ByteArrayId dataId : dataIds) {
			if (Arrays.equals(
					rawDataId,
					dataId.getBytes())) {
				return true;
			}
		}

		return false;
	}

	@Override
	public List<InputSplit> getSplits(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		return splitsProvider.getSplits(
				operations,
				query,
				queryOptions,
				adapterStore,
				statsStore,
				indexStore,
				indexMappingStore,
				minSplits,
				maxSplits);
	}
}
