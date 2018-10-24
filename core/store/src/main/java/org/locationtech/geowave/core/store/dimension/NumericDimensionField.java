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
package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

/**
 * This interface provides in addition to the index dimension definition, a way
 * to read and write a field and get a field ID
 *
 * @param <T>
 */
public interface NumericDimensionField<T extends CommonIndexValue> extends
		NumericDimensionDefinition
{
	/**
	 * Decode a numeric value or range from the raw field value
	 *
	 * @param dataElement
	 *            the raw field value
	 * @return a numeric value or range
	 */
	public NumericData getNumericData(
			T dataElement );

	/**
	 * Returns an identifier that is unique for a given data type (field IDs
	 * should be distinct per row)
	 *
	 * @return the field name
	 */
	public String getFieldName();

	/**
	 * Get a writer that can handle serializing values for this field
	 *
	 * @return the field writer for this field
	 */
	public FieldWriter<?, T> getWriter();

	/**
	 * Get a reader that can handle deserializing binary data into values for
	 * this field
	 *
	 * @return the field reader for this field
	 */
	public FieldReader<T> getReader();

	/**
	 * Get the basic index definition for this field
	 *
	 * @return the base index definition for this dimension
	 */
	public NumericDimensionDefinition getBaseDefinition();
}
