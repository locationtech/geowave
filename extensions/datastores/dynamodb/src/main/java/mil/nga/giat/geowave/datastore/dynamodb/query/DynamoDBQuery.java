package mil.nga.giat.geowave.datastore.dynamodb.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.DataStoreQuery;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBDataStore;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;
import mil.nga.giat.geowave.datastore.dynamodb.util.AsyncPaginatedQuery;
import mil.nga.giat.geowave.datastore.dynamodb.util.AsyncPaginatedScan;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedQuery;
import mil.nga.giat.geowave.datastore.dynamodb.util.LazyPaginatedScan;

/**
 * This class is used internally to perform query operations against an DynamoDB
 * data store. The query is defined by the set of parameters passed into the
 * constructor.
 */
abstract public class DynamoDBQuery extends
		DataStoreQuery
{
	private final static Logger LOGGER = Logger.getLogger(DynamoDBQuery.class);
	final DynamoDBOperations dynamodbOperations;

	public DynamoDBQuery(
			final BaseDataStore dataStore,
			final DynamoDBOperations dynamodbOperations,
			final PrimaryIndex index,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this(
				dataStore,
				dynamodbOperations,
				null,
				index,
				null,
				visibilityCounts,
				authorizations);
	}

	public DynamoDBQuery(
			final BaseDataStore dataStore,
			final DynamoDBOperations dynamodbOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		super(
				dataStore,
				adapterIds,
				index,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);

		this.dynamodbOperations = dynamodbOperations;
	}

	protected Iterator<Map<String, AttributeValue>> getResults(
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit,
			final AdapterStore adapterStore ) {
		return getResults(
				maxResolutionSubsamplingPerDimension,
				limit,
				adapterStore,
				false);
	}

	protected Iterator<Map<String, AttributeValue>> getResults(
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit,
			final AdapterStore adapterStore,
			final boolean async) {
		final List<ByteArrayRange> ranges = getRanges();
		final String tableName = dynamodbOperations.getQualifiedTableName(
				StringUtils.stringFromBinary(
						index.getId().getBytes()));
		if ((ranges != null) && !ranges.isEmpty()) {
			final List<QueryRequest> requests = new ArrayList<>();
			if ((ranges.size() == 1) && (adapterIds.size() == 1)) {
				final List<QueryRequest> queries = getPartitionRequests(
						tableName);
				final ByteArrayRange r = ranges.get(
						0);
				if (r.isSingleValue()) {
					for (final QueryRequest query : queries) {
						for (final ByteArrayId adapterID : adapterIds) {
							final byte[] start = ByteArrayUtils.combineArrays(
									adapterID.getBytes(),
									r.getStart().getBytes());
							query.addQueryFilterEntry(
									DynamoDBRow.GW_RANGE_KEY,
									new Condition()
											.withAttributeValueList(
													new AttributeValue().withB(
															ByteBuffer.wrap(
																	start)))
											.withComparisonOperator(
													ComparisonOperator.EQ));
						}
					}
				}
				else {
					for (final QueryRequest query : queries) {
						for (final ByteArrayId adapterID : adapterIds) {
							addQueryRange(
									r,
									query,
									adapterID);
						}
					}
				}
				requests.addAll(
						queries);
			}
			else {
				ranges.forEach(
						(r -> requests.addAll(
								addQueryRanges(
										tableName,
										r,
										adapterIds,
										adapterStore))));
			}
            if(async){
                return Iterators.concat(
                        requests.parallelStream().map(
                                this::executeAsyncQueryRequest).iterator()); 
            }
            else{
                return Iterators.concat(
                        requests.parallelStream().map(
                                this::executeQueryRequest).iterator()); 
            }

		}
		else if ((adapterIds != null) && !adapterIds.isEmpty()) {
			final List<QueryRequest> queries = getAdapterOnlyQueryRequests(
					tableName);
		}
		
		if(async){
			final ScanRequest request = new ScanRequest(
					tableName);
			return new AsyncPaginatedScan(
					request,
					dynamodbOperations.getClient());
		}
		else{
			// query everything
			final ScanRequest request = new ScanRequest(
					tableName);
			final ScanResult scanResult = dynamodbOperations.getClient().scan(
					request);
			return new LazyPaginatedScan(
					scanResult,
					request,
					dynamodbOperations.getClient());
		}

	}

	private List<QueryRequest> getAdapterOnlyQueryRequests(
			final String tableName ) {
		final List<QueryRequest> allQueries = new ArrayList<>();

		for (final ByteArrayId adapterId : adapterIds) {
			final List<QueryRequest> singleAdapterQueries = getPartitionRequests(tableName);
			final byte[] start = adapterId.getBytes();
			final byte[] end = adapterId.getNextPrefix();
			for (final QueryRequest queryRequest : singleAdapterQueries) {
				queryRequest.addKeyConditionsEntry(
						DynamoDBRow.GW_RANGE_KEY,
						new Condition().withComparisonOperator(
								ComparisonOperator.BETWEEN).withAttributeValueList(
								new AttributeValue().withB(ByteBuffer.wrap(start)),
								new AttributeValue().withB(ByteBuffer.wrap(end))));
			}
			allQueries.addAll(singleAdapterQueries);
		}
		return allQueries;
	}

	private List<QueryRequest> addQueryRanges(
			final String tableName,
			final ByteArrayRange r,
			final List<ByteArrayId> adapterIds,
			final AdapterStore adapterStore ) {
		List<QueryRequest> retVal = null;
		if (adapterIds.isEmpty()) {
			final CloseableIterator<DataAdapter<?>> adapters = adapterStore.getAdapters();
			final List<ByteArrayId> adapterIDList = new ArrayList<ByteArrayId>();
			adapters.forEachRemaining(new Consumer<DataAdapter<?>>() {
				@Override
				public void accept(
						final DataAdapter<?> t ) {
					adapterIDList.add(t.getAdapterId());
				}
			});
			adapterIds.addAll(adapterIDList);
		}

		for (final ByteArrayId adapterId : adapterIds) {
			final List<QueryRequest> internalRequests = getPartitionRequests(tableName);
			for (final QueryRequest queryRequest : internalRequests) {
				addQueryRange(
						r,
						queryRequest,
						adapterId);
			}
			if (retVal == null) {
				retVal = internalRequests;
			}
			else {
				retVal.addAll(internalRequests);
			}
		}
		if (retVal == null) {
			return Collections.EMPTY_LIST;
		}
		return retVal;
	}

	private void addQueryRange(
			final ByteArrayRange r,
			final QueryRequest query,
			final ByteArrayId adapterID ) {
		final byte[] start = ByteArrayUtils.combineArrays(
				adapterID.getBytes(),
				r.getStart().getBytes());
		final byte[] end = ByteArrayUtils.combineArrays(
				adapterID.getBytes(),
				r.getEndAsNextPrefix().getBytes());
		query.addKeyConditionsEntry(
				DynamoDBRow.GW_RANGE_KEY,
				new Condition().withComparisonOperator(
						ComparisonOperator.BETWEEN).withAttributeValueList(
						new AttributeValue().withB(ByteBuffer.wrap(start)),
						new AttributeValue().withB(ByteBuffer.wrap(end))));
	}

	private static List<QueryRequest> getPartitionRequests(
			final String tableName ) {
		final List<QueryRequest> requests = new ArrayList<>(
				DynamoDBDataStore.PARTITIONS);
		for (long p = 0; p < (DynamoDBDataStore.PARTITIONS); p++) {
			requests.add(new QueryRequest(
					tableName).addKeyConditionsEntry(
					DynamoDBRow.GW_PARTITION_ID_KEY,
					new Condition().withComparisonOperator(
							ComparisonOperator.EQ).withAttributeValueList(
							new AttributeValue().withN(Long.toString(p)))));
		}
		return requests;
	}

	private Iterator<Map<String, AttributeValue>> executeQueryRequest(
			final QueryRequest queryRequest ) {
		final QueryResult result = dynamodbOperations.getClient().query(
				queryRequest);
		return new LazyPaginatedQuery(
				result,
				queryRequest,
				dynamodbOperations.getClient());
	}

	/**
	 * Asynchronous version of the query request. Does not block
	 */
	public Iterator<Map<String, AttributeValue>> executeAsyncQueryRequest(
			final QueryRequest queryRequest ) {
		return new AsyncPaginatedQuery(
				queryRequest,
				dynamodbOperations.getClient());
	}

}
