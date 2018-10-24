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

import java.util.Set;

public interface PartitionIndexStrategy<QueryRangeType extends IndexConstraints, EntryRangeType> extends
		IndexStrategy
{
	public Set<ByteArray> getInsertionPartitionKeys(
			EntryRangeType insertionData );

	public Set<ByteArray> getQueryPartitionKeys(
			QueryRangeType queryData,
			IndexMetaData... hints );

	/***
	 * Get the offset in bytes before the dimensional index. This can accounts
	 * for tier IDs and bin IDs
	 * 
	 * @return the byte offset prior to the dimensional index
	 */
	public int getPartitionKeyLength();

	public Set<ByteArray> getPredefinedSplits();

}
