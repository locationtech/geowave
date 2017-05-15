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
package mil.nga.giat.geowave.cli.osm.osmfeature.types.attributes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttributeDefinition
{
	public String type = null;
	public String name = null;
	public String key = null;
	public final Map<String, List<String>> args = new HashMap<>();

	public Object convert(
			Object obj ) {
		return AttributeTypes.getAttributeType(
				type).convert(
				obj);
	}
}
