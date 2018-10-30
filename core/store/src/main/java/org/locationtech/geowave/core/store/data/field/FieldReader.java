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
package org.locationtech.geowave.core.store.data.field;

import java.util.function.Function;

/**
 * This interface deserializes a field from binary data
 *
 * @param <FieldType>
 */
public interface FieldReader<FieldType> extends
		Function<byte[], FieldType>
{

	/**
	 * Deserializes the field from binary data
	 *
	 * @param fieldData
	 *            The binary serialization of the data object
	 * @return The deserialization of the entry
	 */
	public FieldType readField(
			byte[] fieldData );

	@Override
	default FieldType apply(
			final byte[] fieldData ) {
		return readField(
				fieldData);
	}

}
