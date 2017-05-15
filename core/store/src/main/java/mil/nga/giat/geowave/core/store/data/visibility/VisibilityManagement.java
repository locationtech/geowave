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
package mil.nga.giat.geowave.core.store.data.visibility;

import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;

/**
 * Provides a single consolidated tool to determine the visibility for a
 * specific field.
 */
public interface VisibilityManagement<T>
{
	/**
	 * Create a visibility handler
	 * 
	 * @param fieldName
	 *            -- the name of the field for object of type T requiring
	 *            visibility treatment
	 * @param defaultHandler
	 *            -- default handler if the visibilityAttributeName is not
	 *            provided or the field does not exist in a provided object of
	 *            type T.
	 * @param visibilityAttributeName
	 *            -- optional name of a field that determines visibility of each
	 *            field in an object of type T
	 * @return
	 */
	public FieldVisibilityHandler<T, Object> createVisibilityHandler(
			final String fieldName,
			FieldVisibilityHandler<T, Object> defaultFieldVisiblityHandler,
			final String visibilityAttribute );

}
