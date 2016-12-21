package mil.nga.giat.geowave.datastore.dynamodb.query;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.ConstraintsQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;

/**
 * This class represents basic numeric contraints applied to an DynamoDB Query
 *
 */
public class DynamoDBConstraintsQuery extends
		DynamoDBFilteredIndexQuery
{
	private static final int MAX_RANGE_DECOMPOSITION = -1;
	protected final ConstraintsQuery base;
	private boolean queryFiltersEnabled;

	public DynamoDBConstraintsQuery(
			final DynamoDBOperations dynamodbOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		this(
				dynamodbOperations,
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(index.getIndexStrategy()) : null,
				query != null ? query.createFilters(index.getIndexModel()) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				fieldIdsAdapterPair,
				indexMetaData,
				duplicateCounts,
				visibilityCounts,
				authorizations);
	}

	public DynamoDBConstraintsQuery(
			final DynamoDBOperations dynamodbOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {

		super(
				dynamodbOperations,
				adapterIds,
				index,
				queryFilters,
				clientDedupeFilter,
				scanCallback,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);

		base = new ConstraintsQuery(
				constraints,
				aggregation,
				indexMetaData,
				index,
				queryFilters,
				clientDedupeFilter,
				duplicateCounts,
				this);

		queryFiltersEnabled = true;
	}

	@Override
	protected boolean isAggregation() {
		return base.isAggregation();
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		return DataStoreUtils.constraintsToByteArrayRanges(
				base.constraints,
				index.getIndexStrategy(),
				MAX_RANGE_DECOMPOSITION,
				base.indexMetaData);
	}

	public boolean isQueryFiltersEnabled() {
		return queryFiltersEnabled;
	}

	public void setQueryFiltersEnabled(
			final boolean queryFiltersEnabled ) {
		this.queryFiltersEnabled = queryFiltersEnabled;
	}

	@Override
	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final Iterator<Map<String, AttributeValue>> results ) {
		if (isAggregation()) {
			// aggregate the stats to a single value here
			Mergeable mergedAggregationResult = null;
			if (!results.hasNext()) {
				return Iterators.emptyIterator();
			}
			else {
				while (results.hasNext()) {
					final Map<String, AttributeValue> input = results.next();
					if (input.get(DynamoDBRow.GW_VALUE_KEY) != null) {
						if (mergedAggregationResult == null) {
							mergedAggregationResult = PersistenceUtils.fromBinary(
									input.get(
											DynamoDBRow.GW_VALUE_KEY).getB().array(),
									Mergeable.class);
						}
						else {
							mergedAggregationResult.merge(PersistenceUtils.fromBinary(
									input.get(
											DynamoDBRow.GW_VALUE_KEY).getB().array(),
									Mergeable.class));
						}
					}
				}
			}
			return Iterators.singletonIterator(mergedAggregationResult);
		}
		else {
			return super.initIterator(
					adapterStore,
					results);
		}
	}
}
