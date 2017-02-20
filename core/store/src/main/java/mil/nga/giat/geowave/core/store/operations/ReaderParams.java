package mil.nga.giat.geowave.core.store.operations;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public class ReaderParams extends
		BaseReaderParams
{
	private final boolean isServersideAggregation;
	private final QueryRanges queryRanges;
	private final DistributableQueryFilter filter;
	private final List<MultiDimensionalCoordinateRangesArray> coordinateRanges;
	private final List<MultiDimensionalNumericData> constraints;

	public ReaderParams(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldSubsets,
			final boolean isMixedVisibility,
			final boolean isServersideAggregation,
			final QueryRanges queryRanges,
			final DistributableQueryFilter filter,
			final Integer limit,
			final List<MultiDimensionalCoordinateRangesArray> coordinateRanges,
			final List<MultiDimensionalNumericData> constraints,
			final String... additionalAuthorizations ) {
		super(
				index,
				adapterIds,
				maxResolutionSubsamplingPerDimension,
				aggregation,
				fieldSubsets,
				isMixedVisibility,
				limit,
				additionalAuthorizations);
		this.isServersideAggregation = isServersideAggregation;
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
