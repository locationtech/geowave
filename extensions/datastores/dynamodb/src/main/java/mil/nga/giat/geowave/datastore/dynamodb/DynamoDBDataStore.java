package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.dynamodb.index.secondary.DynamoDBSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBAdapterStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBDataStatisticsStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBIndexStore;
import mil.nga.giat.geowave.datastore.dynamodb.query.DynamoDBConstraintsQuery;
import mil.nga.giat.geowave.datastore.dynamodb.query.DynamoDBRowIdsQuery;
import mil.nga.giat.geowave.datastore.dynamodb.query.DynamoDBRowPrefixQuery;

public class DynamoDBDataStore extends
		BaseDataStore
{
	public final static String TYPE = "dynamodb";
	public static final Integer PARTITIONS = 1;

	private final static Logger LOGGER = Logger.getLogger(DynamoDBDataStore.class);
	private final DynamoDBOperations dynamodbOperations;
	private static int counter = 0;

	public DynamoDBDataStore(
			final DynamoDBOperations operations ) {
		super(
				new DynamoDBIndexStore(
						operations),
				new DynamoDBAdapterStore(
						operations),
				new DynamoDBDataStatisticsStore(
						operations),
				new DynamoDBAdapterIndexMappingStore(
						operations),
				new DynamoDBSecondaryIndexDataStore(
						operations),
				operations,
				operations.getOptions().getBaseOptions());
		dynamodbOperations = operations;
	}

	@Override
	protected IndexWriter createIndexWriter(
			final DataAdapter adapter,
			final PrimaryIndex index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback callback,
			final Closeable closable ) {
		return new DynamoDBIndexWriter<>(
				this,
				adapter,
				index,
				dynamodbOperations,
				callback,
				closable);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {
		// TODO
	}

	@Override
	protected CloseableIterator<Object> queryConstraints(
			final List<ByteArrayId> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final DynamoDBConstraintsQuery dynamodbQuery = new DynamoDBConstraintsQuery(
				this,
				dynamodbOperations,
				adapterIdsToQuery,
				index,
				sanitizedQuery,
				filter,
				(ScanCallback<Object, DynamoDBRow>) sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getAggregation(),
				sanitizedQueryOptions.getFieldIdsAdapterPair(),
				IndexMetaDataSet.getIndexMetadata(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				DuplicateEntryCount.getDuplicateCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());

		return dynamodbQuery.query(
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	@Override
	protected CloseableIterator<Object> queryRowPrefix(
			final PrimaryIndex index,
			final ByteArrayId rowPrefix,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> adapterIdsToQuery ) {
		final DynamoDBRowPrefixQuery<Object> prefixQuery = new DynamoDBRowPrefixQuery<Object>(
				this,
				dynamodbOperations,
				index,
				rowPrefix,
				(ScanCallback<Object, DynamoDBRow>) sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getLimit(),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());
		return prefixQuery.query(
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				tempAdapterStore);
	}

	@Override
	protected CloseableIterator<Object> queryRowIds(
			final DataAdapter<Object> adapter,
			final PrimaryIndex index,
			final List<ByteArrayId> rowIds,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final DynamoDBRowIdsQuery<Object> q = new DynamoDBRowIdsQuery<Object>(
				this,
				dynamodbOperations,
				adapter,
				index,
				rowIds,
				(ScanCallback<Object, DynamoDBRow>) sanitizedQueryOptions.getScanCallback(),
				filter,
				sanitizedQueryOptions.getAuthorizations());

		return q.query(
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	@Override
	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object, Object> scanCallback,
			final DedupeFilter dedupeFilter,
			final String... authorizations ) {
		final Iterator<DynamoDBRow> it = dynamodbOperations.getRows(
				index.getId().getString(),
				Lists.transform(
						dataIds,
						new Function<ByteArrayId, byte[]>() {
							@Override
							public byte[] apply(
									final ByteArrayId input ) {
								return input.getBytes();
							}
						}).toArray(
						new byte[][] {}),
				adapter.getAdapterId().getBytes(),
				authorizations);
		return new CloseableIterator.Wrapper<>(
				new NativeEntryIteratorWrapper<Object>(
						this,
						adapterStore,
						index,
						it,
						null,
						(ScanCallback) scanCallback,
						true));
	}

	@Override
	protected List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {
		// TODO

		return Collections.EMPTY_LIST;
	}

	@Override
	protected boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations ) {
		return false;
	}

	@Override
	protected Deleter createIndexDeleter(
			final String indexTableName,
			final boolean isAltIndex,
			final String... authorizations )
			throws Exception {
		return new DynamoDBRowDeleter(
				dynamodbOperations,
				indexTableName,
				authorizations);
	}

	@Override
	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		// TODO Auto-generated method stub

	}

	@Override
	protected Iterable<GeoWaveRow> getRowsFromIngest(
			byte[] adapterId,
			DataStoreEntryInfo ingestInfo,
			List<FieldInfo<?>> fieldInfoList,
			boolean ensureUniqueId ) {
		final List<GeoWaveRow> rows = new ArrayList<GeoWaveRow>();

		// The single FieldInfo contains the fieldMask in the ID, and the
		// flattened fields in the written value
		byte[] fieldMask = fieldInfoList.get(
				0).getDataValue().getId().getBytes();
		byte[] value = fieldInfoList.get(
				0).getWrittenValue();

		Iterator<ByteArrayId> rowIdIterator = ingestInfo.getRowIds().iterator();

		for (final ByteArrayId insertionId : ingestInfo.getInsertionIds()) {
			final byte[] insertionIdBytes = insertionId.getBytes();
			byte[] uniqueDataId;
			if (ensureUniqueId) {
				uniqueDataId = DataStoreUtils.ensureUniqueId(
						ingestInfo.getDataId(),
						false).getBytes();
			}
			else {
				uniqueDataId = ingestInfo.getDataId();
			}

			// for each insertion(index) id, there's a matching rowId
			// that contains the duplicate count
			GeoWaveRow tempRow = new GeoWaveKeyImpl(
					rowIdIterator.next().getBytes());
			int numDuplicates = tempRow.getNumberOfDuplicates();

			rows.add(new DynamoDBRow(
					nextPartitionId(),
					uniqueDataId,
					adapterId,
					insertionIdBytes,
					fieldMask,
					value,
					numDuplicates));
		}

		return rows;
	}

	private String nextPartitionId() {
		counter = (counter + 1) % PARTITIONS;

		return Integer.toString(counter);
	}

	@Override
	public void write(
			Writer writer,
			Iterable<GeoWaveRow> rows,
			String unused ) {
		final List<WriteRequest> mutations = new ArrayList<WriteRequest>();

		for (GeoWaveRow row : rows) {
			final Map<String, AttributeValue> map = new HashMap<String, AttributeValue>();

			String partitionId = ((DynamoDBRow) row).getPartitionId();

			byte[] rowId = row.getRowId();
			final ByteBuffer rangeKeyBuffer = ByteBuffer.allocate(rowId.length);
			rangeKeyBuffer.put(rowId);
			rangeKeyBuffer.rewind();

			final ByteBuffer fieldMaskBuffer = ByteBuffer.allocate(row.getFieldMask().length);
			fieldMaskBuffer.put(row.getFieldMask());
			fieldMaskBuffer.rewind();

			final ByteBuffer valueBuffer = ByteBuffer.allocate(row.getValue().length);
			valueBuffer.put(row.getValue());
			valueBuffer.rewind();

			map.put(
					DynamoDBRow.GW_PARTITION_ID_KEY,
					new AttributeValue().withN(partitionId));

			map.put(
					DynamoDBRow.GW_RANGE_KEY,
					new AttributeValue().withB(rangeKeyBuffer));

			map.put(
					DynamoDBRow.GW_FIELD_MASK_KEY,
					new AttributeValue().withB(fieldMaskBuffer));

			map.put(
					DynamoDBRow.GW_VALUE_KEY,
					new AttributeValue().withB(valueBuffer));

			mutations.add(new WriteRequest(
					new PutRequest(
							map)));
		}

		writer.write(mutations);
	}
}