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
package mil.nga.giat.geowave.core.store;

public interface GenericStoreFactory<T> extends
		GenericFactory
{
	/**
	 * Create the store, w/the options instance that was returned and populated
	 * w/createOptionsInstance().
	 */
	T createStore(
			StoreFactoryOptions options );

	/**
	 * An object used to configure the specific store. This really exists so
	 * that the command line options for JCommander can be filled in without
	 * knowing which options class we specifically have to create.
	 */
	StoreFactoryOptions createOptionsInstance();

}
