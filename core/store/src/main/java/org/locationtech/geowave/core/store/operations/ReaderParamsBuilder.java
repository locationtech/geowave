package org.locationtech.geowave.core.store.operations;

import java.util.List;

import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class ReaderParamsBuilder<T> extends
		BaseReaderParamsBuilder<T, ReaderParamsBuilder<T>>
{

	protected boolean isServersideAggregation = false;
	protected boolean isClientsideRowMerging = false;
	protected QueryRanges queryRanges = null;
	protected QueryFilter filter = null;
	protected List<MultiDimensionalCoordinateRangesArray> coordinateRanges = null;
	protected List<MultiDimensionalNumericData> constraints = null;

	public ReaderParamsBuilder(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final GeoWaveRowIteratorTransformer<T> rowTransformer ) {
		super(
				index,
				adapterStore,
				internalAdapterStore,
				rowTransformer);
	}

	@Override
	protected ReaderParamsBuilder<T> builder() {
		return this;
	}

	public ReaderParamsBuilder<T> isServersideAggregation(
			final boolean isServersideAggregation ) {
		this.isServersideAggregation = isServersideAggregation;
		return builder();
	}

	public ReaderParamsBuilder<T> isClientsideRowMerging(
			final boolean isClientsideRowMerging ) {
		this.isClientsideRowMerging = isClientsideRowMerging;
		return builder();
	}

	public ReaderParamsBuilder<T> queryRanges(
			final QueryRanges queryRanges ) {
		this.queryRanges = queryRanges;
		return builder();
	}

	public ReaderParamsBuilder<T> filter(
			final QueryFilter filter ) {
		this.filter = filter;
		return builder();
	}

	public ReaderParamsBuilder<T> coordinateRanges(
			final List<MultiDimensionalCoordinateRangesArray> coordinateRanges ) {
		this.coordinateRanges = coordinateRanges;
		return builder();
	}

	public ReaderParamsBuilder<T> constraints(
			final List<MultiDimensionalNumericData> constraints ) {
		this.constraints = constraints;
		return builder();
	}

	public ReaderParams<T> build() {
		if (queryRanges == null) {
			queryRanges = new QueryRanges();
		}
		if (additionalAuthorizations == null) {
			additionalAuthorizations = new String[0];
		}
		return new ReaderParams<T>(
				index,
				adapterStore,
				internalAdapterStore,
				adapterIds,
				maxResolutionSubsamplingPerDimension,
				aggregation,
				fieldSubsets,
				isMixedVisibility,
				isAuthorizationsLimiting,
				isServersideAggregation,
				isClientsideRowMerging,
				queryRanges,
				filter,
				limit,
				maxRangeDecomposition,
				coordinateRanges,
				constraints,
				rowTransformer,
				additionalAuthorizations);
	}
}
