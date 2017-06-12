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
package mil.nga.giat.geowave.cli.osm.osmfeature;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import mil.nga.giat.geowave.cli.osm.osmfeature.types.attributes.AttributeDefinition;
import mil.nga.giat.geowave.cli.osm.osmfeature.types.features.FeatureDefinition;
import mil.nga.giat.geowave.cli.osm.osmfeature.types.features.FeatureDefinitionSet;
import mil.nga.giat.geowave.cli.osm.osmfeature.types.features.FeatureType;

public class FeatureConfigParser
{
	public void parseConfig(
			InputStream configStream )
			throws IOException {

		ObjectMapper om = new ObjectMapper();

		JsonNode rootNode = om.readTree(configStream);

		JsonNode tables = rootNode.path("tables");

		Iterator<Map.Entry<String, JsonNode>> nodeIterator = tables.fields();
		while (nodeIterator.hasNext()) {
			Map.Entry<String, JsonNode> feature = nodeIterator.next();
			FeatureDefinition fd = parseFeature(
					feature.getKey(),
					feature.getValue());
			FeatureDefinitionSet.Features.add(fd);
		}

	}

	private static FeatureDefinition parseFeature(
			String name,
			JsonNode node ) {
		FeatureDefinition fd = new FeatureDefinition();
		fd.name = name;
		Iterator<Map.Entry<String, JsonNode>> featureIterator = node.fields();
		while (featureIterator.hasNext()) {
			Map.Entry<String, JsonNode> props = featureIterator.next();
			switch (props.getKey()) {
				case "fields": {
					parseFields(
							props.getValue(),
							fd);
					break;
				}
				case "type": {
					switch (props.getValue().asText()) {
						case "polygon": {
							fd.type = FeatureType.Polygon;
							break;
						}
						case "linestring": {
							fd.type = FeatureType.LineString;
							break;
						}
						case "point": {
							fd.type = FeatureType.Point;
							break;
						}
						case "geometry": {
							fd.type = FeatureType.Geometry;
							break;
						}
						case "validated_geometry": {
							fd.type = FeatureType.Geometry;
							break;
						}
						default:
							break;
					}
					break;
				}
				case "mapping": {
					parseMapping(
							props.getValue(),
							fd);
					break;
				}
				case "mappings": {
					parseSubMappings(
							props.getValue(),
							fd);
					break;
				}
				case "filters": {
					parseFilters(
							props.getValue(),
							fd);
					break;
				}
				default:
					break;
			}
		}
		return fd;
	}

	private static void parseFilters(
			JsonNode node,
			FeatureDefinition fd ) {
		Iterator<Map.Entry<String, JsonNode>> filterIter = node.fields();
		while (filterIter.hasNext()) {
			Map.Entry<String, JsonNode> filterKVP = filterIter.next();
			Map<String, List<String>> filter = new HashMap<>();
			List<String> filterVals = new ArrayList<>();
			for (JsonNode filterVal : filterKVP.getValue()) {
				filterVals.add(filterVal.asText());
			}
			filter.put(
					filterKVP.getKey(),
					filterVals);
			fd.filters.add(filter);
		}
	}

	private static void parseMapping(
			JsonNode node,
			FeatureDefinition fd ) {
		Iterator<Map.Entry<String, JsonNode>> mappingIter = node.fields();
		while (mappingIter.hasNext()) {
			Map.Entry<String, JsonNode> mapKVP = mappingIter.next();
			final List<String> mapValues = new ArrayList<>();
			for (JsonNode mapVal : mapKVP.getValue()) {
				mapValues.add(mapVal.asText());
			}
			fd.mappings.put(
					mapKVP.getKey(),
					mapValues);
			fd.mappingKeys.add(mapKVP.getKey());
		}
	}

	private static void parseSubMappings(
			JsonNode node,
			FeatureDefinition fd ) {
		Iterator<Map.Entry<String, JsonNode>> mappingsIter = node.fields();
		while (mappingsIter.hasNext()) {
			Map.Entry<String, JsonNode> mappingsKVP = mappingsIter.next();
			for (JsonNode mapping : mappingsKVP.getValue()) {
				Iterator<Map.Entry<String, JsonNode>> mappIter = mapping.fields();
				while (mappIter.hasNext()) {
					Map.Entry<String, JsonNode> mappKVP = mappIter.next();
					final Map<String, List<String>> submapping = new HashMap<>();
					final List<String> submappingValues = new ArrayList<>();
					for (JsonNode subMapVal : mappKVP.getValue()) {
						submappingValues.add(subMapVal.asText());
					}
					submapping.put(
							mappKVP.getKey(),
							submappingValues);
					if (!fd.subMappings.containsKey(mappingsKVP.getKey())) {
						fd.subMappings.put(
								mappingsKVP.getKey(),
								new ArrayList<Map<String, List<String>>>());
					}
					fd.subMappings.get(
							mappingsKVP.getKey()).add(
							submapping);
					fd.mappingKeys.add(mappKVP.getKey());
				}

			}

		}
	}

	private static void parseFields(
			JsonNode node,
			FeatureDefinition fd ) {
		for (JsonNode attr : node) {
			Iterator<Map.Entry<String, JsonNode>> fieldIterator = attr.fields();
			final AttributeDefinition ad = new AttributeDefinition();
			while (fieldIterator.hasNext()) {
				Map.Entry<String, JsonNode> field = fieldIterator.next();
				switch (field.getKey()) {
					case "type": {
						ad.type = field.getValue().asText();
						break;
					}
					case "name": {
						ad.name = field.getValue().asText();
						break;
					}
					case "key": {
						ad.key = field.getValue().asText();
						break;
					}
					case "args": {
						Iterator<Map.Entry<String, JsonNode>> argsIterator = field.getValue().fields();
						while (argsIterator.hasNext()) {
							Map.Entry<String, JsonNode> arg = argsIterator.next();
							List<String> allArgs = new ArrayList<>();
							for (JsonNode item : arg.getValue()) {
								allArgs.add(item.asText());
							}
							ad.args.put(
									arg.getKey(),
									allArgs);
						}
						break;
					}
				}
			}
			fd.attributes.add(ad);
		}

	}

}
