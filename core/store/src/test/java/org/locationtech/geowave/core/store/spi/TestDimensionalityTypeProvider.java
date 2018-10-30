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
package org.locationtech.geowave.core.store.spi;

import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeOptions;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeProviderSpi;

public class TestDimensionalityTypeProvider implements
		DimensionalityTypeProviderSpi
{

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return null;
	}

	@Override
	public String getDimensionalityTypeName() {
		return "test";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return null;
	}

	@Override
	public int getPriority() {
		return 0;
	}

	@Override
	public Index createIndex(
			DimensionalityTypeOptions options ) {
		return null;
	}

	@Override
	public DimensionalityTypeOptions createOptions() {
		return null;
	}

}
