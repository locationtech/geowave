/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.query.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.FloatCompareUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.BinnedNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * This filter can perform fine-grained acceptance testing on generic
 * dimensions, but is limited to only using MBR (min-max in a single dimension,
 * hyper-cubes in multi-dimensional space)
 * 
 */
public class BasicQueryFilter implements
		QueryFilter
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

	protected Map<ByteArray, List<MultiDimensionalNumericData>> binnedConstraints;
	protected NumericDimensionField<?>[] dimensionFields;
	// this is referenced for serialization purposes only
	protected MultiDimensionalNumericData constraints;
	protected BasicQueryCompareOperation compareOp = BasicQueryCompareOperation.INTERSECTS;

	public BasicQueryFilter() {}

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

		binnedConstraints = new HashMap<ByteArray, List<MultiDimensionalNumericData>>();
		this.constraints = constraints;
		final List<BinnedNumericDataset> queries = BinnedNumericDataset.applyBins(
				constraints,
				dimensionFields);
		for (final BinnedNumericDataset q : queries) {
			final ByteArray binId = new ByteArray(
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
		final List<BinnedNumericDataset> dataRanges = BinnedNumericDataset.applyBins(
				((CommonIndexedPersistenceEncoding) persistenceEncoding).getNumericData(dimensionFields),
				dimensionFields);
		// check that at least one data range overlaps at least one query range
		for (final BinnedNumericDataset dataRange : dataRanges) {
			final List<MultiDimensionalNumericData> queries = binnedConstraints.get(new ByteArray(
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
		final byte[][] lengthDimensionAndQueryBinaries = new byte[dimensions][];
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
			lengthDimensionAndQueryBinaries[d] = buf.array();
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
			dimensionFields[d] = (NumericDimensionField<?>) PersistenceUtils.fromBinary(field);
		}
		constraints = new BasicNumericDataset(
				data);
		init(
				constraints,
				dimensionFields);
	}
}
