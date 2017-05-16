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
package mil.nga.giat.geowave.format.geotools.vector;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public interface RetypingVectorDataPlugin
{
	public RetypingVectorDataSource getRetypingSource(
			SimpleFeatureType type );

	public static interface RetypingVectorDataSource
	{
		public SimpleFeatureType getRetypedSimpleFeatureType();

		public SimpleFeature getRetypedSimpleFeature(
				SimpleFeatureBuilder retypeBuilder,
				SimpleFeature original );
	}
}
