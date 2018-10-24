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
package org.locationtech.geowave.adapter.vector.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.geotime.util.SimpleFeatureUserDataConfiguration;
import org.locationtech.geowave.core.index.StringUtils;
import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleFeaturePrimaryIndexConfiguration implements
		SimpleFeatureUserDataConfiguration,
		java.io.Serializable
{
	private static final long serialVersionUID = -7425830022998223202L;
	public static final String INDEX_NAME = "PrimaryIndexName";
	private List<String> indexNames = null;

	public SimpleFeaturePrimaryIndexConfiguration() {
		super();
	}

	public SimpleFeaturePrimaryIndexConfiguration(
			final SimpleFeatureType type ) {
		super();
		configureFromType(type);
	}

	/**
	 * Get all the index names associated with the SimpleFeatureType referenced.
	 *
	 * @param type
	 *            - SFT object which contains Index Names
	 * @return List of index names
	 */
	public static final List<String> getIndexNames(
			final SimpleFeatureType type ) {
		final Object obj = type.getUserData().get(
				INDEX_NAME);
		if (obj != null) {
			return Arrays.asList(obj.toString().split(
					","));
		}
		return Collections.emptyList();
	}

	/**
	 * {@inheritDoc} This method updates the passed in type by adding a CSV
	 * string of all the index names for this Simple Feature Primary Index
	 * Configuration. It is stored in user data as '{@value #INDEX_NAME}'
	 *
	 * @param SFT
	 *            to be updated.
	 */
	@Override
	public void updateType(
			final SimpleFeatureType type ) {
		final StringBuffer names = new StringBuffer();
		if (indexNames == null) {
			return;
		}
		for (final String name : indexNames) {
			if (names.length() > 0) {
				names.append(",");
			}
			names.append(name);

		}
		type.getUserData().put(
				INDEX_NAME,
				names.toString());
	}

	@Override
	public void configureFromType(
			final SimpleFeatureType type ) {
		indexNames = getIndexNames(type);
	}

	public List<String> getIndexNames() {
		return indexNames;
	}

	public void setIndexNames(
			final List<String> indexNames ) {
		this.indexNames = indexNames;
	}

	@Override
	public byte[] toBinary() {
		return StringUtils.stringsToBinary(indexNames.toArray(new String[0]));
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		indexNames = Arrays.asList(StringUtils.stringsFromBinary(bytes));
	}

}
