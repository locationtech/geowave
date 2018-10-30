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
package org.locationtech.geowave.core.index;

import java.util.List;

import org.locationtech.geowave.core.index.persist.Persistable;

/**
 * Interface which defines an index strategy.
 * 
 */
public interface IndexStrategy<QueryRangeType extends IndexConstraints, EntryRangeType> extends
		Persistable
{
	public List<IndexMetaData> createMetaData();

	/**
	 *
	 * @return a unique ID associated with the index strategy
	 */
	public String getId();
}
