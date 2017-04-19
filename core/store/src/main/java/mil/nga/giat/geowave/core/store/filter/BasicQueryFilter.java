package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.FloatCompareUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.BinnedNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

/**
 * This filter can perform fine-grained acceptance testing on generic
 * dimensions, but is limited to only using MBR (min-max in a single dimension,
 * hyper-cubes in multi-dimensional space)
 * 
 */
public class BasicQueryFilter implements
		DistributableQueryFilter
{

	protected interface BasicQueryCompareOp
	{
		public boolean compare(
				double dataMin,
				double dataMax,
				double queryMin,
				double queryMax );
	}

	public enum BasicQueryCompareOperation
			implements
			BasicQueryCompareOp {
		CONTAINS {
			@Override
			public boolean compare(
					double dataMin,
					double dataMax,
					double queryMin,
					double queryMax ) {
				// checking if data range contains query range
				return !((dataMin < queryMin) || (dataMax > queryMax));
			}
		},
		OVERLAPS {
			@Override
			public boolean compare(
					double dataMin,
					double dataMax,
					double queryMin,
					double queryMax ) {
				// per definition, it shouldn't allow only boundary points to
				// overlap (stricter than intersect, see DE-9IM definitions)
				return !((dataMax <= queryMin) || (dataMin >= queryMax)) && !EQUALS.compare(
						dataMin,
						dataMax,
						queryMin,
						queryMax) && !CONTAINS.compare(
						dataMin,
						dataMax,
						queryMin,
						queryMax) && !WITHIN.compare(
						dataMin,
						dataMax,
						queryMin,
						queryMax);
			}
		},
		INTERSECTS {
			@Override
			public boolean compare(
					double dataMin,
					double dataMax,
					double queryMin,
					double queryMax ) {
				// similar to overlap but a bit relaxed (allows boundary points
				// to touch)
				// this is equivalent to !((dataMax < queryMin) || (dataMin >
				// queryMax));
				return !DISJOINT.compare(
						dataMin,
						dataMax,
						queryMin,
						queryMax);
			}
		},
		TOUCHES {
			@Override
			public boolean compare(
					double dataMin,
					double dataMax,
					double queryMin,
					double queryMax ) {
				return (FloatCompareUtils.checkDoublesEqual(
						dataMin,
						queryMax)) || (FloatCompareUtils.checkDoublesEqual(
						dataMax,
						queryMin));
			}
		},
		WITHIN {
			@Override
			public boolean compare(
					double dataMin,
					double dataMax,
					double queryMin,
					double queryMax ) {
				// checking if query range is within the data range
				// this is equivalent to (queryMin >= dataMin) && (queryMax <=
				// dataMax);
				return CONTAINS.compare(
						queryMin,
						queryMax,
						dataMin,
						dataMax);
			}
		},
		DISJOINT {
			@Override
			public boolean compare(
					double dataMin,
					double dataMax,
					double queryMin,
					double queryMax ) {
				return ((dataMax < queryMin) || (dataMin > queryMax));
			}
		},
		CROSSES {
			@Override
			public boolean compare(
					double dataMin,
					double dataMax,
					double queryMin,
					double queryMax ) {
				// accordingly to the def. intersection point must be interior
				// to both source geometries.
				// this is not possible in 1D data so always returns false
				return false;
			}
		},
		EQUALS {
			@Override
			public boolean compare(
					double dataMin,
					double dataMax,
					double queryMin,
					double queryMax ) {
				return (FloatCompareUtils.checkDoublesEqual(
						dataMin,
						queryMin)) && (FloatCompareUtils.checkDoublesEqual(
						dataMax,
						queryMax));
			}
		}
	};

	protected Map<ByteArrayId, List<MultiDimensionalNumericData>> binnedConstraints;
	protected NumericDimensionField<?>[] dimensionFields;
	// this is referenced for serialization purposes only
	protected MultiDimensionalNumericData constraints;
	protected BasicQueryCompareOperation compareOp = BasicQueryCompareOperation.INTERSECTS;

	protected BasicQueryFilter() {}

	public BasicQueryFilter(
			final MultiDimensionalNumericData constraints,
			final NumericDimensionField<?>[] dimensionFields ) {
		init(
				constraints,
				dimensionFields);
	}

	public BasicQueryFilter(
			final MultiDimensionalNumericData constraints,
			final NumericDimensionField<?>[] dimensionFields,
			final BasicQueryCompareOperation compareOp ) {
		init(
				constraints,
				dimensionFields);
		this.compareOp = compareOp;
	}

	private void init(
			final MultiDimensionalNumericData constraints,
			final NumericDimensionField<?>[] dimensionFields ) {
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

	protected boolean validateConstraints(
			final BasicQueryCompareOp op,
			final MultiDimensionalNumericData queryRange,
			final MultiDimensionalNumericData dataRange ) {
		final NumericData[] queryRangePerDimension = queryRange.getDataPerDimension();
		final double[] minPerDimension = dataRange.getMinValuesPerDimension();
		final double[] maxPerDimension = dataRange.getMaxValuesPerDimension();
		boolean ok = true;
		for (int d = 0; d < dimensionFields.length && ok; d++) {
			ok &= op.compare(
					minPerDimension[d],
					maxPerDimension[d],
					queryRangePerDimension[d].getMin(),
					queryRangePerDimension[d].getMax());
		}
		return ok;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		if (!(persistenceEncoding instanceof CommonIndexedPersistenceEncoding)) return false;
		final BinnedNumericDataset[] dataRanges = BinnedNumericDataset.applyBins(
				((CommonIndexedPersistenceEncoding) persistenceEncoding).getNumericData(dimensionFields),
				dimensionFields);
		// check that at least one data range overlaps at least one query range
		for (final BinnedNumericDataset dataRange : dataRanges) {
			final List<MultiDimensionalNumericData> queries = binnedConstraints.get(new ByteArrayId(
					dataRange.getBinId()));
			if (queries != null) {
				for (final MultiDimensionalNumericData query : queries) {
					if ((query != null) && validateConstraints(
							compareOp,
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
		int byteBufferLength = 8;
		final int dimensions = Math.min(
				constraints.getDimensionCount(),
				dimensionFields.length);
		final List<byte[]> lengthDimensionAndQueryBinaries = new ArrayList<byte[]>(
				dimensions);
		final NumericData[] dataPerDimension = constraints.getDataPerDimension();
		for (int d = 0; d < dimensions; d++) {
			final NumericDimensionField<?> dimension = dimensionFields[d];
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
		buf.putInt(this.compareOp.ordinal());
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
		this.compareOp = BasicQueryCompareOperation.values()[buf.getInt()];
		final int numDimensions = buf.getInt();
		dimensionFields = new NumericDimensionField<?>[numDimensions];
		final NumericData[] data = new NumericData[numDimensions];
		for (int d = 0; d < numDimensions; d++) {
			final byte[] field = new byte[buf.getInt()];
			data[d] = new NumericRange(
					buf.getDouble(),
					buf.getDouble());
			buf.get(field);
			dimensionFields[d] = PersistenceUtils.fromBinary(
					field,
					NumericDimensionField.class);
		}
		constraints = new BasicNumericDataset(
				data);
		init(
				constraints,
				dimensionFields);
	}
}
