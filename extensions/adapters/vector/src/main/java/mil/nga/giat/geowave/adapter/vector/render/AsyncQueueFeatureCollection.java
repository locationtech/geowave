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
package mil.nga.giat.geowave.adapter.vector.render;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.collection.BaseSimpleFeatureCollection;
import org.geotools.feature.collection.DelegateSimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.aol.cyclops.data.async.Queue;

public class AsyncQueueFeatureCollection extends
		BaseSimpleFeatureCollection
{
	private final Queue<SimpleFeature> asyncQueue;

	public AsyncQueueFeatureCollection(
			final SimpleFeatureType type,
			final Queue<SimpleFeature> asyncQueue ) {
		super(
				type);
		this.asyncQueue = asyncQueue;
	}

	@Override
	public SimpleFeatureIterator features() {
		return new DelegateSimpleFeatureIterator(
				asyncQueue.stream().iterator());
	}
}
