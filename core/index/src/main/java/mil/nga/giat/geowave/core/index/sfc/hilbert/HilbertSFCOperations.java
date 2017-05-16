/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.index.sfc.hilbert;

import java.math.BigInteger;

import com.google.uzaygezen.core.CompactHilbertCurve;

import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;

/**
 * This interface is used to abstract the details of operations used by the
 * hilbert space filling curve, in particular to enable both primitive-based
 * operations for performance (in cases where the bits of precision can be
 * adequately represented by primitives) and non-primitive based operations for
 * unbounded bits of precision.
 *
 */
public interface HilbertSFCOperations
{
	/**
	 * initialize this set of operations with the given dimension definitions
	 *
	 * @param dimensionDefinitions
	 *            the dimension definitions to use
	 */
	public void init(
			SFCDimensionDefinition[] dimensionDefinitions );

	/**
	 * Convert the raw values (ordered per dimension) to a single SFC value
	 *
	 * @param values
	 *            a raw value per dimension in order
	 * @param compactHilbertCurve
	 *            the compact Hilbert curve to use for the conversion
	 * @param dimensionDefinitions
	 *            a set of dimension definitions to use to normalize the raw
	 *            values
	 * @return the Hilbert SFC value
	 */
	public byte[] convertToHilbert(
			double[] values,
			CompactHilbertCurve compactHilbertCurve,
			SFCDimensionDefinition[] dimensionDefinitions );

	/**
	 * Convert the single SFC value to the ranges of raw values that it
	 * represents
	 *
	 * @param hilbertValue
	 *            the computed hilbert value to invert back to native
	 *            coordinates
	 * @param compactHilbertCurve
	 *            the compact Hilbert curve to use for the conversion
	 * @param dimensionDefinitions
	 *            a set of dimension definitions to use to normalize the raw
	 *            values
	 * @return the ranges of values that the hilbert represents, inclusive on
	 *         start and exclusive on end for each range
	 */
	public MultiDimensionalNumericData convertFromHilbert(
			byte[] hilbertValue,
			CompactHilbertCurve compactHilbertCurve,
			SFCDimensionDefinition[] dimensionDefinitions );

	/**
	 * Convert the single SFC value to the per dimension SFC coordinates that it
	 * represents
	 *
	 * @param hilbertValue
	 *            the computed hilbert value to invert back to integer
	 *            coordinates per dimension
	 * @param compactHilbertCurve
	 *            the compact Hilbert curve to use for the conversion
	 * @param dimensionDefinitions
	 *            a set of dimension definitions to use to determine the bits of
	 *            precision per dimension that is expected in the compact
	 *            hilbert curve
	 *
	 * @return the integer coordinate value per dimension that the given hilbert
	 *         value represents
	 */
	public long[] indicesFromHilbert(
			byte[] hilbertValue,
			CompactHilbertCurve compactHilbertCurve,
			SFCDimensionDefinition[] dimensionDefinitions );

	/**
	 * Decompose the raw range per dimension values into an optimal set of
	 * compact Hilbert SFC ranges
	 *
	 * @param rangePerDimension
	 *            the raw range per dimension
	 * @param compactHilbertCurve
	 *            the compact Hilbert curve to use for the conversion
	 * @param dimensionDefinitions
	 *            a set of dimension definitions to use to normalize the raw
	 *            values
	 * @param totalPrecision
	 *            the total precision of the dimension definitions, for
	 *            convenience
	 * @param maxFilteredIndexedRanges
	 *            the maximum number of ranges, if < 0 it will be unlimited
	 * @param removeVacuum
	 *            a flag to pass to the compact hilbert curve range
	 *            decomposition
	 * @return the optimal SFC range decomposition for the raw-valued ranges
	 */
	public RangeDecomposition decomposeRange(
			NumericData[] rangePerDimension,
			CompactHilbertCurve compactHilbertCurve,
			SFCDimensionDefinition[] dimensionDefinitions,
			int totalPrecision,
			int maxFilteredIndexedRanges,
			boolean removeVacuum,
			boolean overInclusiveOnEdge );

	/**
	 * Get a quick (minimal complexity calculation) estimate of the total row
	 * IDs a particular data would require to fully cover with SFC values
	 *
	 * @param data
	 *            the dataset
	 * @param dimensionDefinitions
	 *            a set of dimension definitions to use to normalize the raw
	 *            values
	 * @return the total estimated row IDs the data would require to fully cover
	 *         with SFC values
	 */
	public BigInteger getEstimatedIdCount(
			MultiDimensionalNumericData data,
			SFCDimensionDefinition[] dimensionDefinitions );

	/**
	 * Determines the coordinates per dimension of rows given a
	 * multi-dimensional range will span within this space filling curve
	 *
	 * @param minValue
	 *            the minimum value
	 *
	 * @param maxValue
	 *            the maximum value
	 * @param dimension
	 *            the ordinal of the dimension on this space filling curve
	 * @param dimensionDefinitions
	 *            a set of dimension definitions to use to normalize the raw
	 *            values
	 * @return the range of coordinates in each dimension (ie. [0][0] would be
	 *         the min coordinate of the first dimension and [0][1] would be the
	 *         max coordinate of the first dimension)
	 *
	 */
	public long[] normalizeRange(
			double minValue,
			double maxValue,
			int dimension,
			final SFCDimensionDefinition boundedDimensionDefinition )
			throws IllegalArgumentException;

	/***
	 * Get the range/size of a single insertion ID for each dimension
	 *
	 * @param dimensionDefinitions
	 *            a set of dimension definitions to use to calculate the range
	 *            of each insertion ID
	 * @return the range of a single insertion ID for each dimension
	 */
	public double[] getInsertionIdRangePerDimension(
			SFCDimensionDefinition[] dimensionDefinitions );
}
