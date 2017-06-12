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

import java.io.IOException;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface DataStoreOperations
{

	public boolean tableExists(
			final String altIdxTableName )
			throws IOException;

	public void deleteAll()
			throws Exception;

	public String getTableNameSpace();

	public boolean mergeData(
			PrimaryIndex index,
			AdapterStore adapterStore,
			AdapterIndexMappingStore adapterIndexMappingStore );

}
