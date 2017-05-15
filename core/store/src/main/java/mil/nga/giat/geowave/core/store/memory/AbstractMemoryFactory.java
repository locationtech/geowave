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
package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

abstract public class AbstractMemoryFactory implements
		GenericFactory
{
	@Override
	public String getType() {
		return "memory";
	}

	@Override
	public String getDescription() {
		return "A GeoWave store that is in memory typically only used for test purposes";
	}

	/**
	 * Return the default options instance. This is actually a method that
	 * should be implemented by the individual factories, but is placed here
	 * since it's the same.
	 * 
	 * @return
	 */
	public StoreFactoryOptions createOptionsInstance() {
		return new MemoryRequiredOptions();
	}
}
