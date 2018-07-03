package mil.nga.giat.geowave.datastore.dynamodb.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowMergingIterator;
import mil.nga.giat.geowave.core.store.filter.ClientVisibilityFilter;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow.GuavaRowTranslationHelper;
import mil.nga.giat.geowave.datastore.dynamodb.util.AsyncPaginatedQuery;
import mil.nga.giat.geowave.datastore.dynamodb.util.AsyncPaginatedScan;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBUtils;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedQuery;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedScan;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class DynamoDBReader implements
		Reader
{
	private final static Logger LOGGER = Logger.getLogger(DynamoDBReader.class);
	private static final boolean ASYNC = false;
	private final ReaderParams readerParams;
	private final RecordReaderParams recordReaderParams;
	private final DynamoDBOperations operations;
	private Iterator<DynamoDBRow> iterator;

	private ClientVisibilityFilter visibilityFilter;

	public DynamoDBReader(
			final ReaderParams readerParams,
			final DynamoDBOperations operations ) {
		this.readerParams = readerParams;
		recordReaderParams = null;
		processAuthorizations(readerParams.getAdditionalAuthorizations());
		this.operations = operations;
		initScanner();
	}

	public DynamoDBReader(
			final RecordReaderParams recordReaderParams,
			final DynamoDBOperations operations ) {
		readerParams = null;
		this.recordReaderParams = recordReaderParams;
		processAuthorizations(recordReaderParams.getAdditionalAuthorizations());
		this.operations = operations;

		initRecordScanner();
	}

	private void processAuthorizations(
			final String[] authorizations ) {
		visibilityFilter = new ClientVisibilityFilter(
				Sets.newHashSet(authorizations));
	}

	protected void initScanner() {
		final String tableName = operations.getQualifiedTableName(
				readerParams.getIndex().getId().getString());

		// if ((readerParams.getLimit() != null) && (readerParams.getLimit() >
		// 0)) {
		// TODO: we should do something here
		// }

		final List<QueryRequest> requests = new ArrayList<>();

		final Collection<SinglePartitionQueryRanges> ranges = readerParams.getQueryRanges().getPartitionQueryRanges();

		if ((ranges != null) && !ranges.isEmpty()) {
			ranges.forEach(
					(queryRequest -> requests.addAll(
							addQueryRanges(
									tableName,
									queryRequest,
									readerParams.getAdapterIds(),
									readerParams.getAdapterStore()))));

		}
		// else if ((readerParams.getAdapterIds() != null) &&
		// !readerParams.getAdapterIds().isEmpty()) {
		// //TODO this isn't going to work because there aren't partition keys
		// being passed along
		// requests.addAll(
		// getAdapterOnlyQueryRequests(
		// tableName,
		// readerParams.getAdapterIds()));
		// }

		startRead(
				requests,
				tableName);
	}

	protected void initRecordScanner() {
		final String tableName = operations.getQualifiedTableName(recordReaderParams.getIndex().getId().getString());

		final ArrayList<Short> adapterIds = Lists.newArrayList();
		if ((recordReaderParams.getAdapterIds() != null) && !recordReaderParams.getAdapterIds().isEmpty()) {
			for (final Short adapterId : recordReaderParams.getAdapterIds()) {
				adapterIds.add(adapterId);
			}
		}

		final List<QueryRequest> requests = new ArrayList<>();

		final GeoWaveRowRange range = recordReaderParams.getRowRange();
		for (final Short adapterId : adapterIds) {
			final ByteArrayId startKey = range.isInfiniteStartSortKey() ? null : new ByteArrayId(
					range.getStartSortKey());
			final ByteArrayId stopKey = range.isInfiniteStopSortKey() ? null : new ByteArrayId(
					range.getEndSortKey());
			requests.add(getQuery(
					tableName,
					range.getPartitionKey(),
					new ByteArrayRange(
							startKey,
							stopKey),
					adapterId));
		}
		startRead(
				requests,
				tableName);
	}

	private void startRead(
			final List<QueryRequest> requests,
			final String tableName ) {
		Iterator<Map<String, AttributeValue>> rawIterator;
		Predicate<DynamoDBRow> adapterIdFilter = null;
		if (!requests.isEmpty()) {
			if (ASYNC) {
				rawIterator = Iterators.concat(
						requests.parallelStream().map(
								this::executeAsyncQueryRequest).iterator());
			}
			else {
				rawIterator = Iterators.concat(
						requests.parallelStream().map(
								this::executeQueryRequest).iterator());
			}
		}
		else {
			if (ASYNC) {
				final ScanRequest request = new ScanRequest(
						tableName);
				rawIterator = new AsyncPaginatedScan(
						request,
						operations.getClient());
			}
			else {
				// query everything
				final ScanRequest request = new ScanRequest(
						tableName);
				final ScanResult scanResult = operations.getClient().scan(
						request);
				rawIterator = new LazyPaginatedScan(
						scanResult,
						request,
						operations.getClient());
				// TODO it'd be best to keep the set of partitions as a stat and
				// use it to query by adapter IDs server-side
				// but stats could be disabled so we may need to do client-side
				// filtering by adapter ID
				if ((readerParams.getAdapterIds() != null) && !readerParams.getAdapterIds().isEmpty()) {
					adapterIdFilter = new Predicate<DynamoDBRow>() {

						@Override
						public boolean apply(
								final DynamoDBRow input ) {
							return readerParams.getAdapterIds().contains(
											input.getInternalAdapterId());
						}

					};
				}
			}
		}

		iterator = new GeoWaveRowMergingIterator<DynamoDBRow>(
				Iterators.filter(
						Iterators.transform(
								rawIterator,
								new DynamoDBRow.GuavaRowTranslationHelper()),
						visibilityFilter));
		if (adapterIdFilter != null) {
			iterator = Iterators.filter(
					iterator,
					adapterIdFilter);
		}
	}

	@Override
	public void close()
			throws Exception {

	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public GeoWaveRow next() {
		return iterator.next();
	}

	private List<QueryRequest> getAdapterOnlyQueryRequests(
			final String tableName,
			final ArrayList<Short> internalAdapterIds ) {
		final List<QueryRequest> allQueries = new ArrayList<>();

		for (final short internalAdapterId : internalAdapterIds) {
			final QueryRequest singleAdapterQuery = new QueryRequest(
					tableName);

			final byte[] start = ByteArrayUtils.shortToByteArray(internalAdapterId);
			final byte[] end = new ByteArrayId(
					start).getNextPrefix();
			singleAdapterQuery.addKeyConditionsEntry(
					DynamoDBRow.GW_RANGE_KEY,
					new Condition().withComparisonOperator(
							ComparisonOperator.BETWEEN).withAttributeValueList(
							new AttributeValue().withB(ByteBuffer.wrap(start)),
							new AttributeValue().withB(ByteBuffer.wrap(end))));

			allQueries.add(singleAdapterQuery);
		}

		return allQueries;
	}

	private QueryRequest getQuery(
			final String tableName,
			final byte[] partitionId,
			final ByteArrayRange sortRange,
			final short internalAdapterId ) {
		final byte[] start;
		final byte[] end;
		final QueryRequest query = new QueryRequest(
				tableName).addKeyConditionsEntry(
				DynamoDBRow.GW_PARTITION_ID_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.EQ).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(partitionId))));
		if (sortRange == null) {
			start = ByteArrayUtils.shortToByteArray(internalAdapterId);
			end = new ByteArrayId(
					start).getNextPrefix();
		}
		else if (sortRange.isSingleValue()) {
			start = ByteArrayUtils.combineArrays(
					ByteArrayUtils.shortToByteArray(internalAdapterId),
					DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getBytes()));
			end = ByteArrayUtils.combineArrays(
					ByteArrayUtils.shortToByteArray(internalAdapterId),
					DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getNextPrefix()));
		}
		else {
			if (sortRange.getStart() == null) {
				start = ByteArrayUtils.shortToByteArray(internalAdapterId);
			}
			else {
				start = ByteArrayUtils.combineArrays(
						ByteArrayUtils.shortToByteArray(internalAdapterId),
						DynamoDBUtils.encodeSortableBase64(sortRange.getStart().getBytes()));
			}
			if (sortRange.getEnd() == null) {
				end = new ByteArrayId(
						ByteArrayUtils.shortToByteArray(internalAdapterId)).getNextPrefix();
			}
			else {
				end = ByteArrayUtils.combineArrays(
						ByteArrayUtils.shortToByteArray(internalAdapterId),
						DynamoDBUtils.encodeSortableBase64(sortRange.getEndAsNextPrefix().getBytes()));
			}
		}
		query.addKeyConditionsEntry(
				DynamoDBRow.GW_RANGE_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.BETWEEN).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(start)),
						new AttributeValue().withB(ByteBuffer.wrap(end))));
		return query;
	}

	private List<QueryRequest> addQueryRanges(
			final String tableName,
			final SinglePartitionQueryRanges r,
			Collection<Short> internalAdapterIds,
			final PersistentAdapterStore adapterStore ) {
		final List<QueryRequest> retVal = new ArrayList<>();
		final ByteArrayId partitionKey = r.getPartitionKey();
		final byte[] partitionId = ((partitionKey == null) || (partitionKey.getBytes().length == 0))
				? DynamoDBWriter.EMPTY_PARTITION_KEY : partitionKey.getBytes();
		if ((internalAdapterIds == null || internalAdapterIds.isEmpty()) && (adapterStore != null)) {
			final CloseableIterator<InternalDataAdapter<?>> adapters = adapterStore.getAdapters();
			internalAdapterIds = new ArrayList<>();
			final List<Short> adapterIDList = new ArrayList<Short>();
			adapters.forEachRemaining(
					new Consumer<InternalDataAdapter<?>>() {
						@Override
						public void accept(
								final InternalDataAdapter<?> t ) {
							adapterIDList.add(
									t.getInternalAdapterId());
						}
					});
			internalAdapterIds.addAll(
					adapterIDList);
		}

		for (final Short internalAdapterId : internalAdapterIds) {
			final Collection<ByteArrayRange> sortKeyRanges = r.getSortKeyRanges();
			if ((sortKeyRanges != null) && !sortKeyRanges.isEmpty()) {
				sortKeyRanges.forEach(
						(sortKeyRange -> retVal.add(
								getQuery(
										tableName,
										partitionId,
										sortKeyRange,
										internalAdapterId))));
			}
			else {
				retVal.add(
						getQuery(
								tableName,
								partitionId,
								null,
								internalAdapterId));
			}
		}
		return retVal;
	}

	private Iterator<Map<String, AttributeValue>> executeQueryRequest(
			final QueryRequest queryRequest ) {
		final QueryResult result = operations.getClient().query(
				queryRequest);
		return new LazyPaginatedQuery(
				result,
				queryRequest,
				operations.getClient());
	}

	/**
	 * Asynchronous version of the query request. Does not block
	 */
	public Iterator<Map<String, AttributeValue>> executeAsyncQueryRequest(
			final QueryRequest queryRequest ) {
		return new AsyncPaginatedQuery(
				queryRequest,
				operations.getClient());
	}
}
