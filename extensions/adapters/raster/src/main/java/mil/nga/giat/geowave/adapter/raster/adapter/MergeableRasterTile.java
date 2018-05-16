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

import java.awt.image.DataBuffer;

import mil.nga.giat.geowave.adapter.raster.adapter.merge.RootMergeStrategy;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.Persistable;

public class MergeableRasterTile<T extends Persistable> extends
		RasterTile<T>
{
	private RootMergeStrategy<T> mergeStrategy;
	private ByteArrayId dataAdapterId;

	public MergeableRasterTile() {
		// this isn't really meant to be persisted, its instantiated using the
		// other constructor for merging purposes only leveraging the
		// RootMergeStrategy (also not persistable)

		// because this implements mergeable though and is technically
		// persistable, this constructor is provided and us registered for
		// consistency
	}

	public MergeableRasterTile(
			final DataBuffer dataBuffer,
			final T metadata,
			final RootMergeStrategy<T> mergeStrategy,
			final ByteArrayId dataAdapterId ) {
		super(
				dataBuffer,
				metadata);
		this.mergeStrategy = mergeStrategy;
		this.dataAdapterId = dataAdapterId;
	}

	public ByteArrayId getDataAdapterId() {
		return dataAdapterId;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((mergeStrategy != null) && (merge != null) && (merge instanceof RasterTile)) {
			mergeStrategy.merge(
					this,
					(RasterTile<T>) merge,
					dataAdapterId);
		}
		else {
			super.merge(merge);
		}
	}
}
