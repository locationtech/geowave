package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray.ArrayOfArrays;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeUtils.RangeCache;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeUtils.RangeLookupFactory;

public class HBaseNumericIndexStrategyFilter extends
		FilterBase
{
	private NumericIndexStrategy indexStrategy;
	private MultiDimensionalCoordinateRangesArray[] coordinateRanges;
	private RangeCache rangeCache;

	public HBaseNumericIndexStrategyFilter() {}

	public HBaseNumericIndexStrategyFilter(
			final NumericIndexStrategy indexStrategy,
			final MultiDimensionalCoordinateRangesArray[] coordinateRanges ) {
		super();
		this.indexStrategy = indexStrategy;
		this.coordinateRanges = coordinateRanges;
		rangeCache = RangeLookupFactory.createMultiRangeLookup(coordinateRanges);
	}

	public static HBaseNumericIndexStrategyFilter parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(pbBytes);
		NumericIndexStrategy indexStrategy;
		MultiDimensionalCoordinateRangesArray[] coordinateRanges;
		try {
			final int indexStrategyLength = buf.getInt();
			final byte[] indexStrategyBytes = new byte[indexStrategyLength];
			buf.get(indexStrategyBytes);
			indexStrategy = PersistenceUtils.fromBinary(
					indexStrategyBytes,
					NumericIndexStrategy.class);
			final byte[] coordRangeBytes = new byte[pbBytes.length - indexStrategyLength - 4];
			buf.get(coordRangeBytes);
			final ArrayOfArrays arrays = new ArrayOfArrays();
			arrays.fromBinary(coordRangeBytes);
			coordinateRanges = arrays.getCoordinateArrays();
		}
		catch (final Exception e) {
			throw new DeserializationException(
					"Unable to read parameters",
					e);
		}

		return new HBaseNumericIndexStrategyFilter(
				indexStrategy,
				coordinateRanges);
	}

	@Override
	public byte[] toByteArray()
			throws IOException {
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
	public ReturnCode filterKeyValue(
			final Cell cell )
			throws IOException {
		if (inBounds(cell)) {
			return ReturnCode.INCLUDE;
		}
		return ReturnCode.SKIP;
	}

	private boolean inBounds(
			final Cell cell ) {
		final MultiDimensionalCoordinates coordinates = indexStrategy.getCoordinatesPerDimension(new ByteArrayId(
				new GeoWaveRowImpl(
						cell.getRowArray(),
						cell.getRowOffset(),
						cell.getRowLength()).getIndex()));
		return rangeCache.inBounds(coordinates);
	}
}
