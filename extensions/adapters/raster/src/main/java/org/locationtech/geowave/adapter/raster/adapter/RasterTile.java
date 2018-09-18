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
package org.locationtech.geowave.adapter.raster.adapter;

import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferDouble;
import java.awt.image.DataBufferFloat;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.ByteDataBuffer;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.DoubleArray;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.DoubleDataBuffer;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.FloatArray;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.FloatDataBuffer;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.SignedIntArray;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.SignedIntDataBuffer;
import org.locationtech.geowave.adapter.raster.util.DataBufferPersistenceUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

import me.lemire.integercompression.differential.IntegratedIntCompressor;

public class RasterTile<T extends Persistable> implements
		Mergeable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RasterTile.class);
	private DataBuffer dataBuffer;
	private T metadata;

	public RasterTile() {
		super();
	}

	public RasterTile(
			final DataBuffer dataBuffer,
			final T metadata ) {
		this.dataBuffer = dataBuffer;
		this.metadata = metadata;
	}

	public DataBuffer getDataBuffer() {
		return dataBuffer;
	}

	public T getMetadata() {
		return metadata;
	}

	@Override
	public byte[] toBinary() {
		final byte[] dataBufferBinary = DataBufferPersistenceUtils.getDataBufferBinary(dataBuffer);
		byte[] metadataBytes;
		if (metadata != null) {
			metadataBytes = PersistenceUtils.toBinary(metadata);
		}
		else {
			metadataBytes = new byte[] {};
		}
		final ByteBuffer buf = ByteBuffer.allocate(metadataBytes.length + dataBufferBinary.length + 4);
		buf.putInt(metadataBytes.length);
		buf.put(metadataBytes);
		buf.put(dataBufferBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		try {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			final int metadataLength = buf.getInt();
			if (metadataLength > 0) {
				final byte[] metadataBytes = new byte[metadataLength];
				buf.get(metadataBytes);
				metadata = (T) PersistenceUtils.fromBinary(metadataBytes);
			}
			final byte[] dataBufferBytes = new byte[bytes.length - metadataLength - 4];
			buf.get(dataBufferBytes);
			dataBuffer = DataBufferPersistenceUtils.getDataBuffer(dataBufferBytes);
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to deserialize data buffer",
					e);
		}
	}

	public void setDataBuffer(
			final DataBuffer dataBuffer ) {
		this.dataBuffer = dataBuffer;
	}

	public void setMetadata(
			final T metadata ) {
		this.metadata = metadata;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		// This will get wrapped as a MergeableRasterTile by the combiner to
		// support merging
	}
}
