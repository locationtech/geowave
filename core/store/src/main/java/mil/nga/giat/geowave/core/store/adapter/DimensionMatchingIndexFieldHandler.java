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

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is internally useful for the AbstractDataAdapter to match field handlers
 * with dimensions by field ID. Any fields with the ID returned by
 * getSupportedIndexFieldIds() will use this handler.
 * 
 * @param <RowType>
 * @param <IndexFieldType>
 * @param <NativeFieldType>
 */
public interface DimensionMatchingIndexFieldHandler<RowType, IndexFieldType extends CommonIndexValue, NativeFieldType> extends
		IndexFieldHandler<RowType, IndexFieldType, NativeFieldType>
{
	/**
	 * Returns the set of field IDs that are supported by this field handler
	 * 
	 * @return the set of field IDs supported
	 */
	public ByteArrayId[] getSupportedIndexFieldIds();
}
