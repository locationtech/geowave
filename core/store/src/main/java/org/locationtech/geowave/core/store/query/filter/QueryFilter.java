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
package org.locationtech.geowave.core.store.query.filter;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * A simple filter interface to determine inclusion/exclusion based on a generic
 * persistence encoding. Client-side filters will be given an
 * AdapterPersistenceEncoding but distributable filters will be given a generic
 * PersistenceEncoding.
 *
 */
public interface QueryFilter extends
		Persistable
{
	public boolean accept(
			CommonIndexModel indexModel,
			IndexedPersistenceEncoding<?> persistenceEncoding );
}
