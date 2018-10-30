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
import java.awt.image.SampleModel;

import org.locationtech.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import org.locationtech.geowave.adapter.raster.adapter.merge.ServerMergeStrategy;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;

public class ClientMergeableRasterTile<T extends Persistable> extends
		RasterTile<T>
{
	private RasterTileMergeStrategy<T> mergeStrategy;
	private SampleModel sampleModel;

	public ClientMergeableRasterTile() {}

	public ClientMergeableRasterTile(
			RasterTileMergeStrategy<T> mergeStrategy,
			SampleModel sampleModel,
			final DataBuffer dataBuffer,
			final T metadata ) {
		super(
				dataBuffer,
				metadata);
		this.mergeStrategy = mergeStrategy;

		this.sampleModel = sampleModel;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((mergeStrategy != null) && (merge != null) && (merge instanceof RasterTile)) {
			mergeStrategy.merge(
					this,
					(RasterTile<T>) merge,
					sampleModel);
		}
		else {
			super.merge(merge);
		}
	}
}
