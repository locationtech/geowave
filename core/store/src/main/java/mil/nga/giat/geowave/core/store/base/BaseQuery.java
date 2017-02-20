package mil.nga.giat.geowave.core.store.base;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

/**
 * This class is used internally to perform query operations against a base data
 * store. The query is defined by the set of parameters passed into the
 * constructor.
 */
abstract class BaseQuery
{
	private final static Logger LOGGER = Logger.getLogger(BaseQuery.class);

	protected final BaseDataStore dataStore;
	protected List<ByteArrayId> adapterIds;
	protected final PrimaryIndex index;
	protected final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair;
	protected final DifferingFieldVisibilityEntryCount visibilityCounts;
	protected final String[] authorizations;

	public BaseQuery(
			final BaseDataStore dataStore,
			final PrimaryIndex index,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this(
				dataStore,
				null,
				index,
				null,
				visibilityCounts,
				authorizations);
	}

	public BaseQuery(
			final BaseDataStore dataStore,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this.dataStore = dataStore;
		this.adapterIds = adapterIds;
		this.index = index;
		this.fieldIdsAdapterPair = fieldIdsAdapterPair;
		this.visibilityCounts = visibilityCounts;
		this.authorizations = authorizations;
	}

	protected Reader getReader(
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		return operations.createReader(new ReaderParams(
				index,
				adapterIds,
				maxResolutionSubsamplingPerDimension,
				getAggregation(),
				getFieldSubsets(),
				isMixedVisibilityRows(),
				isServerSideAggregation(options),
				getRanges(),
				getServerFilter(options),
				limit,
				getCoordinateRanges(),
				getConstraints(),
				getAdditionalAuthorizations()));
	}

	public boolean isServerSideAggregation(
			final DataStoreOptions options ) {
		return ((options != null) && options.isServerSideLibraryEnabled() && isAggregation());
	}

	public boolean isAggregation() {
		final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation = getAggregation();
		return ((aggregation != null) && (aggregation.getRight() != null));
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return null;
	}

	public List<MultiDimensionalNumericData> getConstraints() {
		return null;
	}

	abstract protected QueryRanges getRanges();

	protected Pair<DataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return null;
	}

	protected Pair<List<String>, DataAdapter<?>> getFieldSubsets() {
		return fieldIdsAdapterPair;
	}

	protected boolean isMixedVisibilityRows() {
		return (visibilityCounts == null) || visibilityCounts.isAnyEntryDifferingFieldVisiblity();
	}

	public String[] getAdditionalAuthorizations() {
		return authorizations;
	}

	public DistributableQueryFilter getServerFilter(
			final DataStoreOptions options ) {
		return null;
	}

	protected QueryFilter getClientFilter(
			final DataStoreOptions options ) {
		return null;
	}

	protected boolean isCommonIndexAggregation() {
		return false;
	}
}
