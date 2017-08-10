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
package mil.nga.giat.geowave.analytic;

import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement.BatchIdFilter;
import mil.nga.giat.geowave.analytic.clustering.DistortionGroupManagement.DistortionDataAdapter;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;

public class AnalyticPersistableRegistry implements
		PersistableRegistrySpi
{
	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 700,
					GeoObjectDimensionValues::new),
			new PersistableIdAndConstructor(
					(short) 701,
					BatchIdFilter::new),
			new PersistableIdAndConstructor(
					(short) 702,
					DistortionDataAdapter::new),
			new PersistableIdAndConstructor(
					(short) 703,
					PersistableStore::new)
		};
	}
}
