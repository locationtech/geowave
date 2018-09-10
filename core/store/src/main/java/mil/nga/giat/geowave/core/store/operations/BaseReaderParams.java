package mil.nga.giat.geowave.core.store.operations;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

abstract public class BaseReaderParams<T>
{

	private final PrimaryIndex index;
	private final PersistentAdapterStore adapterStore;
	private final Collection<Short> adapterIds;
	private final double[] maxResolutionSubsamplingPerDimension;
	private final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	private final Pair<List<String>, InternalDataAdapter<?>> fieldSubsets;
	private final boolean isMixedVisibility;
	private final boolean isAuthorizationsLimiting;
	private final Integer limit;
	private final Integer maxRangeDecomposition;
	private final GeoWaveRowIteratorTransformer<T> rowTransformer;
	private final String[] additionalAuthorizations;

	public BaseReaderParams(
			final PrimaryIndex index,
			final PersistentAdapterStore adapterStore,
			final Collection<Short> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, InternalDataAdapter<?>> fieldSubsets,
			final boolean isMixedVisibility,
			final boolean isAuthorizationsLimiting,
			final Integer limit,
			final Integer maxRangeDecomposition,
			final GeoWaveRowIteratorTransformer<T> rowTransformer,
			final String... additionalAuthorizations ) {
		this.index = index;
		this.adapterStore = adapterStore;
		this.adapterIds = adapterIds;
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
		this.aggregation = aggregation;
		this.fieldSubsets = fieldSubsets;
		this.isMixedVisibility = isMixedVisibility;
		this.isAuthorizationsLimiting = isAuthorizationsLimiting;
		this.limit = limit;
		this.maxRangeDecomposition = maxRangeDecomposition;
		this.rowTransformer = rowTransformer;
		this.additionalAuthorizations = additionalAuthorizations;
	}

	public PrimaryIndex getIndex() {
		return index;
	}

	public PersistentAdapterStore getAdapterStore() {
		return adapterStore;
	}

	public Collection<Short> getAdapterIds() {
		return adapterIds;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregation;
	}

	public Pair<List<String>, InternalDataAdapter<?>> getFieldSubsets() {
		return fieldSubsets;
	}

	public boolean isAuthorizationsLimiting() {
		return isAuthorizationsLimiting;
	}

	public boolean isMixedVisibility() {
		return isMixedVisibility;
	}

	public boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getRight() != null));
	}

	public Integer getLimit() {
		return limit;
	}

	public Integer getMaxRangeDecomposition() {
		return maxRangeDecomposition;
	}

	public String[] getAdditionalAuthorizations() {
		return additionalAuthorizations;
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return null;
	}

	public List<MultiDimensionalNumericData> getConstraints() {
		return null;
	}

	public DistributableQueryFilter getFilter() {
		return null;
	}

	public boolean isServersideAggregation() {
		return false;
	}

	public GeoWaveRowIteratorTransformer<T> getRowTransformer() {
		return rowTransformer;
	}
}
