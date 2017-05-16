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
package mil.nga.giat.geowave.core.store.dimension;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

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
	 * @return the field ID
	 */
	public ByteArrayId getFieldId();

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
