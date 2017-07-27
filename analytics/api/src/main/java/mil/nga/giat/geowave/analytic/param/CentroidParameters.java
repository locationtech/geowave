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
package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;

public class CentroidParameters
{
	public enum Centroid
			implements
			ParameterEnum {
		INDEX_ID(
				String.class,
				"cid",
				"Index Identifier for Centroids",
				false,
				true),
		DATA_TYPE_ID(
				String.class,
				"cdt",
				"Data Type ID for a centroid item",
				false,
				true),
		DATA_NAMESPACE_URI(
				String.class,
				"cns",
				"Data Type Namespace for centroid item",
				false,
				true),
		CONXVERGANCE_TOLERANCE(
				Double.class,
				"cct",
				"The alpha parameter measure the minimum covergence to reach before ",
				false,
				true),
		EXTRACTOR_CLASS(
				CentroidExtractor.class,
				"cce",
				"Centroid Exractor Class implements mil.nga.giat.geowave.analytics.extract.CentroidExtractor",
				true,
				true),
		WRAPPER_FACTORY_CLASS(
				AnalyticItemWrapperFactory.class,
				"cfc",
				"A factory class that implements mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory",
				true,
				true),
		ZOOM_LEVEL(
				Integer.class,
				"czl",
				"Zoom Level Number",
				true,
				true);
		private final ParameterHelper helper;

		private Centroid(
				final Class baseClass,
				final String name,
				final String description,
				final boolean isClass,
				final boolean hasArg ) {
			helper = new BasicParameterHelper(
					this,
					baseClass,
					name,
					description,
					isClass,
					hasArg);
		}

		@Override
		public Enum<?> self() {
			return this;
		}

		@Override
		public ParameterHelper getHelper() {
			return helper;
		}

	}

}
