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
package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.data.DataReader;
import mil.nga.giat.geowave.core.store.data.DataWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

/**
 * This interface describes the common fields for all of the data within the
 * index. It is up to data adapters to map (encode) the native fields to these
 * common fields for persistence.
 */
public interface CommonIndexModel extends
		DataReader<CommonIndexValue>,
		DataWriter<Object, CommonIndexValue>,
		Persistable
{
	public NumericDimensionField<? extends CommonIndexValue>[] getDimensions();

	public String getId();
}
