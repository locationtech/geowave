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
package org.locationtech.geowave.test;

import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT.DuplicateCount;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT.DuplicateCountAggregation;
import org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT.MergeCounter;
import org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT.SumAndAveragingMergeStrategy;
import org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT.SummingMergeStrategy;

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
			new PersistableIdAndConstructor(
					(short) 10778,
					DuplicateCountAggregation::new),
		};
	}
}
