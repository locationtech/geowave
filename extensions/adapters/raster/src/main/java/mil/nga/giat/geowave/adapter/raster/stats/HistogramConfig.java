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
package mil.nga.giat.geowave.adapter.raster.stats;

import java.awt.image.SampleModel;
import java.nio.ByteBuffer;

import org.geotools.coverage.TypeMap;
import org.geotools.util.NumberRange;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public class HistogramConfig implements
		Persistable
{
	private static final int MAX_DEFAULT_NUM_BINS = 65536;
	private double[] highValues;
	private double[] lowValues;
	private int[] numBins;

	public HistogramConfig() {}

	public HistogramConfig(
			final SampleModel sampleModel ) {
		final int numBands = sampleModel.getNumBands();
		highValues = new double[numBands];
		lowValues = new double[numBands];
		numBins = new int[numBands];
		for (int b = 0; b < numBands; b++) {
			final NumberRange range = TypeMap.getRange(TypeMap.getSampleDimensionType(
					sampleModel,
					b));
			int bins;
			double min = range.getMinimum(true);
			double max = range.getMaximum(true);
			if (Double.isInfinite(min) || Double.isInfinite(max) || Double.isNaN(min) || Double.isNaN(max)) {
				// in this case there is no reasonable default, just use a range
				// of 0 to 1 as a placeholder
				min = 0;
				max = 1;
				bins = MAX_DEFAULT_NUM_BINS;
			}
			else {
				bins = (int) Math.min(
						MAX_DEFAULT_NUM_BINS,
						(max - min) + 1);
			}
			lowValues[b] = min;
			highValues[b] = max;
			numBins[b] = bins;
		}
	}

	public HistogramConfig(
			final double[] highValues,
			final double[] lowValues,
			final int[] numBins ) {
		this.highValues = highValues;
		this.lowValues = lowValues;
		this.numBins = numBins;
	}

	@Override
	public byte[] toBinary() {
		// constant number of bands, 8 + 8 + 4 bytes per band (high,low, and
		// numBins), and 4 more for the total bands
		final ByteBuffer buf = ByteBuffer.allocate((20 * highValues.length) + 4);
		buf.putInt(highValues.length);
		for (int b = 0; b < highValues.length; b++) {
			buf.putDouble(lowValues[b]);
			buf.putDouble(highValues[b]);
			buf.putInt(numBins[b]);
		}
		return buf.array();
	}

	public double[] getHighValues() {
		return highValues;
	}

	public double[] getLowValues() {
		return lowValues;
	}

	public int[] getNumBins() {
		return numBins;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int numBands = buf.getInt();
		highValues = new double[numBands];
		lowValues = new double[numBands];
		numBins = new int[numBands];
		for (int b = 0; b < numBands; b++) {
			lowValues[b] = buf.getDouble();
			highValues[b] = buf.getDouble();
			numBins[b] = buf.getInt();
		}
	}
}
