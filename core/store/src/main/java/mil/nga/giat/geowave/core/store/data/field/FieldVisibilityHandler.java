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

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * This class must be implemented to perform per field value visibility
 * decisions. The byte array that is returned will be used directly in the
 * visibility column for Accumulo.
 * 
 * @param <RowType>
 * @param <FieldType>
 */
public interface FieldVisibilityHandler<RowType, FieldType>
{
	/**
	 * Determine visibility on a per field basis.
	 * 
	 * @param rowValue
	 *            The value for the full row.
	 * @param fieldId
	 *            The ID of the field for which to determine visibility
	 * @param fieldValue
	 *            The value of the field to determine visibility
	 * @return The visibility for a field
	 */
	public byte[] getVisibility(
			RowType rowValue,
			ByteArrayId fieldId,
			FieldType fieldValue );

}
