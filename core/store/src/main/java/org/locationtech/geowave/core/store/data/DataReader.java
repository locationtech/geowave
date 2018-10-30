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
package org.locationtech.geowave.core.store.data;

import org.locationtech.geowave.core.store.data.field.FieldReader;

/**
 * This interface is used to read data from a row in a GeoWave data store.
 *
 * @param <FieldType>
 *            The binding class of this field
 */
public interface DataReader<FieldType>
{
	/**
	 * Get a reader for an individual field.
	 *
	 * @param fieldName
	 *            the ID of the field
	 * @return the FieldReader for the given field Name (ID)
	 */
	public FieldReader<FieldType> getReader(
			String fieldName );

}
