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
package mil.nga.giat.geowave.core.store.flatten;

import java.util.Comparator;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;

/**
 * Comparator to sort FieldInfo's accordingly. Assumes
 * FieldInfo.getDataValue().getId().getBytes() returns the bitmasked
 * representation of a fieldId
 * 
 * @see BitmaskUtils
 * 
 * @since 0.9.1
 */
public class BitmaskedFieldInfoComparator implements
		Comparator<Pair<Integer, FieldInfo<?>>>,
		java.io.Serializable
{
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(
			final Pair<Integer, FieldInfo<?>> o1,
			final Pair<Integer, FieldInfo<?>> o2 ) {
		return o1.getLeft().compareTo(
				o2.getLeft());
	}

}
