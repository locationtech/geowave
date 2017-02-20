package mil.nga.giat.geowave.core.store.query;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray.ArrayOfArrays;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeUtils.RangeCache;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeUtils.RangeLookupFactory;

public class CoordinateRangeQueryFilter implements
		DistributableQueryFilter
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CoordinateRangeQueryFilter.class);
	protected NumericIndexStrategy indexStrategy;
	protected RangeCache rangeCache;
	protected MultiDimensionalCoordinateRangesArray[] coordinateRanges;

	protected CoordinateRangeQueryFilter() {}

	public CoordinateRangeQueryFilter(
			final NumericIndexStrategy indexStrategy,
			final MultiDimensionalCoordinateRangesArray[] coordinateRanges ) {
		this.indexStrategy = indexStrategy;
		this.coordinateRanges = coordinateRanges;
		rangeCache = RangeLookupFactory.createMultiRangeLookup(coordinateRanges);
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		if ((persistenceEncoding == null)
				|| ((persistenceEncoding.getInsertionPartitionKey() == null) && (persistenceEncoding
						.getInsertionSortKey() == null))) {
			return false;
		}
		return inBounds(
				persistenceEncoding.getInsertionPartitionKey(),
				persistenceEncoding.getInsertionSortKey());
	}

	private boolean inBounds(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		final MultiDimensionalCoordinates coordinates = indexStrategy.getCoordinatesPerDimension(
				partitionKey,
				sortKey);
		return rangeCache.inBounds(coordinates);
	}

	@Override
	public byte[] toBinary() {
		final byte[] indexStrategyBytes = PersistenceUtils.toBinary(indexStrategy);
		final byte[] coordinateRangesBinary = new ArrayOfArrays(
				coordinateRanges).toBinary();

		final ByteBuffer buf = ByteBuffer.allocate(coordinateRangesBinary.length + indexStrategyBytes.length + 4);

		buf.putInt(indexStrategyBytes.length);
		buf.put(indexStrategyBytes);
		buf.put(coordinateRangesBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		try {
			final int indexStrategyLength = buf.getInt();
			final byte[] indexStrategyBytes = new byte[indexStrategyLength];
			buf.get(indexStrategyBytes);
			indexStrategy = PersistenceUtils.fromBinary(
					indexStrategyBytes,
					NumericIndexStrategy.class);
			final byte[] coordRangeBytes = new byte[bytes.length - indexStrategyLength - 4];
			buf.get(coordRangeBytes);
			final ArrayOfArrays arrays = new ArrayOfArrays();
			arrays.fromBinary(coordRangeBytes);
			coordinateRanges = arrays.getCoordinateArrays();
			rangeCache = RangeLookupFactory.createMultiRangeLookup(coordinateRanges);
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to read parameters",
					e);
		}

	}

}
