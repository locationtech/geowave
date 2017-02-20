package mil.nga.giat.geowave.core.store.query;

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;

public class CoordinateRangeQuery implements
		DistributableQuery
{
	private NumericIndexStrategy indexStrategy;
	private MultiDimensionalCoordinateRangesArray[] coordinateRanges;

	public CoordinateRangeQuery() {}

	public CoordinateRangeQuery(
			final NumericIndexStrategy indexStrategy,
			final MultiDimensionalCoordinateRangesArray[] coordinateRanges ) {
		this.indexStrategy = indexStrategy;
		this.coordinateRanges = coordinateRanges;
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		return Collections.singletonList(new CoordinateRangeQueryFilter(
				indexStrategy,
				coordinateRanges));
	}

	@Override
	public boolean isSupported(
			final Index<?, ?> index ) {
		return index.getIndexStrategy().equals(
				indexStrategy);
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		// TODO should we consider implementing this?
		return Collections.EMPTY_LIST;
	}

	@Override
	public byte[] toBinary() {
		return new CoordinateRangeQueryFilter(
				indexStrategy,
				coordinateRanges).toBinary();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final CoordinateRangeQueryFilter filter = new CoordinateRangeQueryFilter();
		filter.fromBinary(bytes);
		indexStrategy = filter.indexStrategy;
		coordinateRanges = filter.coordinateRanges;
	}

	@Override
	public List<ByteArrayRange> getSecondaryIndexConstraints(
			final SecondaryIndex<?> index ) {
		// TODO should we consider implementing this?
		return null;
	}

	@Override
	public List<DistributableQueryFilter> getSecondaryQueryFilter(
			final SecondaryIndex<?> index ) {
		// TODO should we consider implementing this?
		return null;
	}

}
