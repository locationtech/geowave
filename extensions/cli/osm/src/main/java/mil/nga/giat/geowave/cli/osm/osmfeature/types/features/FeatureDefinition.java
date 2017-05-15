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
package mil.nga.giat.geowave.cli.osm.osmfeature.types.features;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.cli.osm.osmfeature.types.attributes.AttributeDefinition;

public class FeatureDefinition
{
	public String name = null;
	public FeatureType type = null;
	public final Map<String, List<String>> mappings = new HashMap<>();
	public final Map<String, List<Map<String, List<String>>>> subMappings = new HashMap<>();
	public final List<AttributeDefinition> attributes = new ArrayList<>();
	public final List<Map<String, List<String>>> filters = new ArrayList<>();
	public final List<String> mappingKeys = new ArrayList<>();
	private static final String WILDCARD_ATTRIBUTE = "__any__";

	public String getMappingName() {
		for (AttributeDefinition ad : attributes) {
			if (ad.type.equals("mapping_value")) {
				return ad.name;
			}
		}
		return null;
	}

	public AttributeDefinition getMappingAttribute() {
		for (AttributeDefinition ad : attributes) {
			if (ad.type.equals("mapping_value")) {
				return ad;
			}
		}
		return null;
	}

	public String getQualifiedSubMappings() {
		for (AttributeDefinition ad : attributes) {
			if (ad.type.equals("mapping_key")) {
				return ad.name;
			}
		}
		return null;
	}

	public AttributeDefinition getSubMappingAttribute() {
		for (AttributeDefinition ad : attributes) {
			if (ad.type.equals("mapping_key")) {
				return ad;
			}
		}
		return null;
	}

	public boolean isMappedValue(
			String val ) {
		for (Map.Entry<String, List<String>> map : mappings.entrySet()) {
			if (map.getValue().contains(
					WILDCARD_ATTRIBUTE) || map.getValue().contains(
					val)) {
				return true;
			}
		}
		return false;
	}

	public String getSubMappingClass(
			String key,
			String val ) {
		for (Map.Entry<String, List<Map<String, List<String>>>> m : subMappings.entrySet()) {
			for (Map<String, List<String>> m2 : m.getValue()) {
				for (Map.Entry<String, List<String>> m3 : m2.entrySet()) {
					if (m3.getKey().equals(
							key)) {
						if (m3.getValue().contains(
								WILDCARD_ATTRIBUTE) || m3.getValue().contains(
								val)) {
							return m3.getKey();
						}
					}
				}
			}
		}
		return null;
	}

}
