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
package org.locationtech.geowave.core.store.data.visibility;

import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;

/**
 * All fields use the same visibility writer
 */

public class UniformVisibilityWriter<RowType> implements
		VisibilityWriter<RowType>
{

	final FieldVisibilityHandler<RowType, Object> uniformHandler;

	public UniformVisibilityWriter(
			final FieldVisibilityHandler<RowType, Object> uniformHandler ) {
		super();
		this.uniformHandler = uniformHandler;
	}

	@Override
	public FieldVisibilityHandler<RowType, Object> getFieldVisibilityHandler(
			final String fieldName ) {

		return uniformHandler;
	}

}
