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

/**
 * This class contains all of the primitive writer field types supported
 *
 */
public class BasicWriter<RowType, FieldType> implements
		FieldWriter<RowType, FieldType>
{
	private FieldVisibilityHandler<RowType, Object> visibilityHandler;
	private FieldWriter<?, FieldType> writer;

	public BasicWriter(
			final FieldWriter<?, FieldType> writer ) {
		this(
				writer,
				null);
	}

	public BasicWriter(
			final FieldWriter<?, FieldType> writer,
			final FieldVisibilityHandler<RowType, Object> visibilityHandler ) {
		this.writer = writer;
		this.visibilityHandler = visibilityHandler;
	}

	@Override
	public byte[] getVisibility(
			final RowType rowValue,
			final String fieldName,
			final FieldType fieldValue ) {
		if (visibilityHandler != null) {
			return visibilityHandler.getVisibility(
					rowValue,
					fieldName,
					fieldValue);
		}
		return new byte[] {};
	}

	@Override
	public byte[] writeField(
			final FieldType fieldValue ) {
		return writer.writeField(fieldValue);
	}

}
