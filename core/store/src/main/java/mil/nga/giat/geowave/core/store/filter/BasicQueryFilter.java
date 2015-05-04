package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.BinnedNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;

/**
 * This filter can perform fine-grained acceptance testing on generic
 * dimensions, but is limited to only using MBR (min-max in a single dimension,
 * hyper-cubes in multi-dimensional space)
 * 
 */
public class BasicQueryFilter implements
		DistributableQueryFilter
{
	protected Map<ByteArrayId, List<MultiDimensionalNumericData>> binnedConstraints;
	protected DimensionField<?>[] dimensionFields;
	// this is referenced for serialization purposes only
	protected MultiDimensionalNumericData constraints;

	protected BasicQueryFilter() {}

	public BasicQueryFilter(
			final MultiDimensionalNumericData constraints,
			final DimensionField<?>[] dimensionFields ) {
		init(
				constraints,
				dimensionFields);
	}

	private void init(
			final MultiDimensionalNumericData constraints,
			final DimensionField<?>[] dimensionFields ) {
		this.dimensionFields = dimensionFields;

		binnedConstraints = new HashMap<ByteArrayId, List<MultiDimensionalNumericData>>();
		this.constraints = constraints;
		final BinnedNumericDataset[] queries = BinnedNumericDataset.applyBins(
				constraints,
				dimensionFields);
		for (final BinnedNumericDataset q : queries) {
			final ByteArrayId binId = new ByteArrayId(
					q.getBinId());
			List<MultiDimensionalNumericData> ranges = binnedConstraints.get(binId);
			if (ranges == null) {
				ranges = new ArrayList<MultiDimensionalNumericData>();
				binnedConstraints.put(
						binId,
						ranges);
			}
			ranges.add(q);
		}
	}

	protected boolean overlaps(
			final MultiDimensionalNumericData queryRange,
			final MultiDimensionalNumericData dataRange ) {
		final NumericData[] queryRangePerDimension = queryRange.getDataPerDimension();
		final double[] minPerDimension = dataRange.getMinValuesPerDimension();
		final double[] maxPerDimension = dataRange.getMaxValuesPerDimension();
		for (int d = 0; d < dimensionFields.length; d++) {
			if ((maxPerDimension[d] < queryRangePerDimension[d].getMin()) || (minPerDimension[d] > queryRangePerDimension[d].getMax())) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean accept(
			final IndexedPersistenceEncoding persistenceEncoding ) {
		final BinnedNumericDataset[] dataRanges = BinnedNumericDataset.applyBins(
				persistenceEncoding.getNumericData(dimensionFields),
				dimensionFields);
		// check that at least one data range overlaps at least one query range
		for (final BinnedNumericDataset dataRange : dataRanges) {
			final List<MultiDimensionalNumericData> queries = binnedConstraints.get(new ByteArrayId(
					dataRange.getBinId()));
			if (queries != null) {
				for (final MultiDimensionalNumericData query : queries) {
					if ((query != null) && overlaps(
							query,
							dataRange)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		int byteBufferLength = 4;
		final int dimensions = Math.min(
				constraints.getDimensionCount(),
				dimensionFields.length);
		final List<byte[]> lengthDimensionAndQueryBinaries = new ArrayList<byte[]>(
				dimensions);
		final NumericData[] dataPerDimension = constraints.getDataPerDimension();
		for (int d = 0; d < dimensions; d++) {
			final DimensionField<?> dimension = dimensionFields[d];
			final NumericData data = dataPerDimension[d];
			final byte[] dimensionBinary = PersistenceUtils.toBinary(dimension);
			final int currentDimensionByteBufferLength = (20 + dimensionBinary.length);

			final ByteBuffer buf = ByteBuffer.allocate(currentDimensionByteBufferLength);
			buf.putInt(dimensionBinary.length);
			buf.putDouble(data.getMin());
			buf.putDouble(data.getMax());
			buf.put(dimensionBinary);
			byteBufferLength += currentDimensionByteBufferLength;
			lengthDimensionAndQueryBinaries.add(buf.array());
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
		buf.putInt(dimensions);
		for (final byte[] binary : lengthDimensionAndQueryBinaries) {
			buf.put(binary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numDimensions = buf.getInt();
		dimensionFields = new DimensionField<?>[numDimensions];
		final NumericData[] data = new NumericData[numDimensions];
		for (int d = 0; d < numDimensions; d++) {
			final byte[] field = new byte[buf.getInt()];
			data[d] = new NumericRange(
					buf.getDouble(),
					buf.getDouble());
			buf.get(field);
			dimensionFields[d] = PersistenceUtils.fromBinary(
					field,
					DimensionField.class);
		}
		constraints = new BasicNumericDataset(
				data);
		init(
				constraints,
				dimensionFields);
	}
}
