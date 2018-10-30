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
package org.locationtech.geowave.core.store.adapter;

import java.util.Map;

import org.locationtech.geowave.core.index.ByteArray;

/**
 * This is used by the AbstractDataAdapter to get individual field values from
 * the row
 *
 * @param <RowType>
 * @param <FieldType>
 */
public interface NativeFieldHandler<RowType, FieldType>
{
	/**
	 * Get the field name that this handler supports
	 *
	 * @return the field name supported
	 */
	public String getFieldName();

	/**
	 * Get the field value from the row
	 *
	 * @param row
	 *            the row
	 * @return the field value
	 */
	public FieldType getFieldValue(
			RowType row );

	/**
	 * This is used internally by the AbstractDataAdapter to build a row from a
	 * set of field values
	 *
	 * @param <RowType>
	 * @param <FieldType>
	 */
	public static interface RowBuilder<RowType, FieldType>
	{
		/**
		 * Set a field name/value pair
		 *
		 * @param fieldValue
		 *            the field ID/value pair
		 */
		public void setField(
				String fieldName,
				FieldType fieldValue );

		public void setFields(
				Map<String, FieldType> values );

		/**
		 * Create a row with the previously set fields
		 *
		 * @param dataId
		 *            the unique data ID for the row
		 * @return the row
		 */
		public RowType buildRow(
				ByteArray dataId );
	}
}
