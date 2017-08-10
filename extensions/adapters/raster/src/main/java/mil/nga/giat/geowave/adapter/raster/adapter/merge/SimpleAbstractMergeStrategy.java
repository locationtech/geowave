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
package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import mil.nga.giat.geowave.adapter.raster.adapter.MergeableRasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.persist.Persistable;

import org.opengis.coverage.grid.GridCoverage;

abstract public class SimpleAbstractMergeStrategy<T extends Persistable> implements
		RasterTileMergeStrategy<T>
{
	protected SimpleAbstractMergeStrategy() {
		super();
	}

	private static final long serialVersionUID = 8937483748317L;

	@Override
	public void merge(
			final RasterTile<T> thisTile,
			final RasterTile<T> nextTile,
			final SampleModel sampleModel ) {
		// this strategy aims for latest tile
		// with data values, but where there
		// is no data in the latest and there is data in the earlier tile, it
		// fills the data from the earlier tile
		if ((nextTile != null) && (nextTile instanceof MergeableRasterTile)) {
			final WritableRaster nextRaster = Raster.createWritableRaster(
					sampleModel,
					nextTile.getDataBuffer(),
					null);
			final WritableRaster thisRaster = Raster.createWritableRaster(
					sampleModel,
					thisTile.getDataBuffer(),
					null);
			mergeRasters(
					thisTile,
					nextTile,
					thisRaster,
					nextRaster);
		}
	}

	protected void mergeRasters(
			final RasterTile<T> thisTile,
			final RasterTile<T> nextTile,
			final WritableRaster thisRaster,
			final WritableRaster nextRaster ) {
		final int maxX = nextRaster.getMinX() + nextRaster.getWidth();
		final int maxY = nextRaster.getMinY() + nextRaster.getHeight();
		for (int b = 0; b < nextRaster.getNumBands(); b++) {
			for (int x = nextRaster.getMinX(); x < maxX; x++) {
				for (int y = nextRaster.getMinY(); y < maxY; y++) {
					final double thisSample = thisRaster.getSampleDouble(
							x,
							y,
							b);

					final double nextSample = nextRaster.getSampleDouble(
							x,
							y,
							b);
					thisRaster.setSample(
							x,
							y,
							b,
							getSample(
									x,
									y,
									b,
									thisSample,
									nextSample));
				}
			}
		}
	}

	abstract protected double getSample(
			int x,
			int y,
			int b,
			double thisSample,
			double nextSample );

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		return true;
	}

	@Override
	public int hashCode() {
		return (int) serialVersionUID;
		// this looks correct based on behaviour of equals?!? should return the
		// same hash code for all instances
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public T getMetadata(
			final GridCoverage tileGridCoverage,
			final RasterDataAdapter dataAdapter ) {
		return null;
	}
}
