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
package mil.nga.giat.geowave.adapter.raster.adapter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class is used by GridCoverageDataAdapter to persist GridCoverages. The
 * adapter has information regarding the sample model and color model so all
 * that is necessary to persist is the buffer and the envelope.
 */
public class GridCoverageWritable implements
		Writable
{
	private RasterTile rasterTile;
	private double minX;
	private double maxX;
	private double minY;
	private double maxY;

	protected GridCoverageWritable() {}

	public GridCoverageWritable(
			final RasterTile rasterTile,
			final double minX,
			final double maxX,
			final double minY,
			final double maxY ) {
		this.rasterTile = rasterTile;
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
	}

	public void set(
			final RasterTile rasterTile,
			final double minX,
			final double maxX,
			final double minY,
			final double maxY ) {
		this.rasterTile = rasterTile;
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
	}

	public RasterTile getRasterTile() {
		return rasterTile;
	}

	public double getMinX() {
		return minX;
	}

	public double getMaxX() {
		return maxX;
	}

	public double getMinY() {
		return minY;
	}

	public double getMaxY() {
		return maxY;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		final int rasterTileSize = input.readInt();
		final byte[] rasterTileBinary = new byte[rasterTileSize];
		input.readFully(rasterTileBinary);
		rasterTile = new RasterTile();
		rasterTile.fromBinary(rasterTileBinary);
		minX = input.readDouble();
		maxX = input.readDouble();
		minY = input.readDouble();
		maxY = input.readDouble();
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		final byte[] rasterTileBinary = rasterTile.toBinary();
		output.writeInt(rasterTileBinary.length);
		output.write(rasterTileBinary);
		output.writeDouble(minX);
		output.writeDouble(maxX);
		output.writeDouble(minY);
		output.writeDouble(maxY);
	}
}
