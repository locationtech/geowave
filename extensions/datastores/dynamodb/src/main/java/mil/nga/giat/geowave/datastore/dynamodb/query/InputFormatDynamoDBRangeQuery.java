package mil.nga.giat.geowave.datastore.dynamodb.query;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;

public class InputFormatDynamoDBRangeQuery extends DynamoDBConstraintsQuery {
	
	public InputFormatDynamoDBRangeQuery(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final DynamoDBOperations dynamoDBOperations,
			final PrimaryIndex index,
//			final Range accumuloRange,
			final List<QueryFilter> queryFilters,
			final boolean isOutputWritable,
			final QueryOptions queryOptions ) {
		
		/**
		 * 	public DynamoDBConstraintsQuery(
			final BaseDataStore dataStore,
			final DynamoDBOperations dynamodbOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, DynamoDBRow> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		 */
		
		super(
				dataStore,
				dynamoDBOperations,
				(List<ByteArrayId>)null,
//				getAdapterIds(
//						index,
//						adapterStore,
//						queryOptions),
				index,
				(Query)null,
//				queryFilters,
				(DedupeFilter) null,
				(ScanCallback<?, DynamoDBRow>) queryOptions.getScanCallback(),
				(Pair<DataAdapter<?>, Aggregation<?, ?, ?>>) null,
				(Pair<List<String>, DataAdapter<?>>) null,
				(IndexMetaData[]) null,
				(DuplicateEntryCount) null,
				(DifferingFieldVisibilityEntryCount) null,
				queryOptions.getAuthorizations());

//		this.accumuloRange = accumuloRange;
//		this.isOutputWritable = isOutputWritable;
	}

}
