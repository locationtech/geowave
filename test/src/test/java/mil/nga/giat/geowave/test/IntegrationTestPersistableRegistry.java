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
package mil.nga.giat.geowave.test;

import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;
import mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT.MergeCounter;
import mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT.SumAndAveragingMergeStrategy;
import mil.nga.giat.geowave.test.basic.GeoWaveBasicRasterIT.SummingMergeStrategy;

public class IntegrationTestPersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 10775,
					SummingMergeStrategy::new),
			new PersistableIdAndConstructor(
					(short) 10776,
					SumAndAveragingMergeStrategy::new),
			new PersistableIdAndConstructor(
					(short) 10777,
					MergeCounter::new),
		};
	}
}
