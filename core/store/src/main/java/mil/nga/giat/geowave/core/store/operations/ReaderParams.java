package mil.nga.giat.geowave.core.store.operations;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public class ReaderParams<T> extends
		BaseReaderParams<T>
{
	private final boolean isServersideAggregation;
	private final boolean isClientsideRowMerging;
	private final QueryRanges queryRanges;
	private final DistributableQueryFilter filter;
	private final List<MultiDimensionalCoordinateRangesArray> coordinateRanges;
	private final List<MultiDimensionalNumericData> constraints;

	public ReaderParams(
			final PrimaryIndex index,
			final PersistentAdapterStore adapterStore,
			final List<Short> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, InternalDataAdapter<?>> fieldSubsets,
			final boolean isMixedVisibility,
			final boolean isServersideAggregation,
			final boolean isClientsideRowMerging,
			final QueryRanges queryRanges,
			final DistributableQueryFilter filter,
			final Integer limit,
			final List<MultiDimensionalCoordinateRangesArray> coordinateRanges,
			final List<MultiDimensionalNumericData> constraints,
			final GeoWaveRowIteratorTransformer<T> rowTransformer,
			final String... additionalAuthorizations ) {
		super(
				index,
				adapterStore,
				adapterIds,
				maxResolutionSubsamplingPerDimension,
				aggregation,
				fieldSubsets,
				isMixedVisibility,
				limit,
				rowTransformer,
				additionalAuthorizations);
		this.isServersideAggregation = isServersideAggregation;
		this.isClientsideRowMerging = isClientsideRowMerging;
		this.queryRanges = queryRanges;
		this.filter = filter;
		this.coordinateRanges = coordinateRanges;
		this.constraints = constraints;
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return coordinateRanges;
	}

	public List<MultiDimensionalNumericData> getConstraints() {
		return constraints;
	}

	public boolean isClientsideRowMerging() {
		return isClientsideRowMerging;
	}

	public boolean isServersideAggregation() {
		return isServersideAggregation;
	}

	public QueryRanges getQueryRanges() {
		return queryRanges;
	}

	public DistributableQueryFilter getFilter() {
		return filter;
	}
}
