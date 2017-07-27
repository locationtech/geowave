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
package mil.nga.giat.geowave.core.store.data.field;

/**
 * This interface serializes a field's value into a byte array
 * 
 * @param <RowType>
 * @param <FieldType>
 */
public interface FieldWriter<RowType, FieldType> extends
		FieldVisibilityHandler<RowType, FieldType>
{

	/**
	 * Serializes the entry into binary data that will be stored as the value
	 * for the row
	 * 
	 * @param fieldValue
	 *            The data object to serialize
	 * @return The binary serialization of the data object
	 */
	public byte[] writeField(
			FieldType fieldValue );

}
