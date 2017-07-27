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
package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

public class ComparisonCellData implements
		WritableComparable<ComparisonCellData>
{
	private double summerPercentile;
	private double winterPercentile;

	public ComparisonCellData() {}

	public ComparisonCellData(
			final double summerPercentile,
			final double winterPercentile ) {
		this.summerPercentile = summerPercentile;
		this.winterPercentile = winterPercentile;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		summerPercentile = input.readDouble();
		winterPercentile = input.readDouble();
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		output.writeDouble(summerPercentile);
		output.writeDouble(winterPercentile);
	}

	public double getSummerPercentile() {
		return summerPercentile;
	}

	public double getWinterPercentile() {
		return winterPercentile;
	}

	public double getCombinedPercentile() {
		return applyCombinationFunction(
				summerPercentile,
				winterPercentile);
	}

	@Override
	public int compareTo(
			final ComparisonCellData other ) {
		final double combined = getCombinedPercentile();
		return Double.compare(
				combined,
				other.getCombinedPercentile());
	}

	@Override
	public boolean equals(
			Object val ) {
		if (!(val instanceof ComparisonCellData)) {
			return false;
		}
		if (val == this) {
			return true;
		}
		return this.compareTo((ComparisonCellData) val) == 0;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(
				2003,
				6373).append(
				summerPercentile).append(
				winterPercentile).toHashCode();
	}

	private static double applyCombinationFunction(
			final double summerPercentile,
			final double winterPercentile ) {
		return summerPercentile - winterPercentile;
	}
}
