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
package org.locationtech.geowave.cli.osm.accumulo.osmschema;

import com.google.common.base.Preconditions;

/**
 *
 */
public class ColumnQualifier
{
	public static final String ID = "-id";
	public static final String LATITUDE = "-lat";
	public static final String LONGITUDE = "-lon";
	public static final String VERSION = "-ver";
	public static final String TIMESTAMP = "-ts";
	public static final String CHANGESET = "-cs";
	public static final String USER_TEXT = "-ut";
	public static final String USER_ID = "-uid";
	public static final String OSM_VISIBILITY = "-vis";
	public static final String REFERENCES = "-ref";
	public static final String REFERENCE_MEMID_PREFIX = "-refmem";
	public static final String REFERENCE_ROLEID_PREFIX = "-refrol";
	public static final String REFERENCE_TYPE_PREFIX = "-reftype";

	public static final String REFERENCE_SEPARATOR = "_";

	public static String getRelationMember(
			final String prefix,
			final int i ) {
		return (prefix + REFERENCE_SEPARATOR + String.valueOf(i));
	}

	public static String TAG_QUALIFIER(
			final String tag ) {
		Preconditions.checkNotNull(tag);
		return tag;
	}
}
