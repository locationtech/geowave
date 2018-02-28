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
package mil.nga.giat.geowave.core.store.spi;

import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

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
	public PrimaryIndex createPrimaryIndex(
			DimensionalityTypeOptions options ) {
		return null;
	}

	@Override
	public DimensionalityTypeOptions createOptions() {
		return null;
	}

}
