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
package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.store.data.DataWriter;

/**
 * This extends the basic DataAdapter interface to be able to ingest data in
 * addition to query for it. Any data adapter used for ingest should implement
 * this interface.
 * 
 * @param <T>
 *            The type of entries that this adapter works on.
 */
public interface WritableDataAdapter<T> extends
		DataAdapter<T>,
		DataWriter<T, Object>
{
}
