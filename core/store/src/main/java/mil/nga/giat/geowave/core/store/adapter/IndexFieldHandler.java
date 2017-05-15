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
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is used by the AbstractDataAdapter to translate between native values
 * and persistence encoded values. The basic implementation of this will perform
 * type matching on the index field type - for explicitly defining the supported
 * dimensions, use DimensionMatchingIndexFieldHandler
 * 
 * @param <RowType>
 * @param <IndexFieldType>
 * @param <NativeFieldType>
 */
public interface IndexFieldHandler<RowType, IndexFieldType extends CommonIndexValue, NativeFieldType>
{
	public ByteArrayId[] getNativeFieldIds();

	public IndexFieldType toIndexValue(
			RowType row );

	public PersistentValue<NativeFieldType>[] toNativeValues(
			IndexFieldType indexValue );
}
