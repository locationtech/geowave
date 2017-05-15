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

import mil.nga.giat.geowave.analytic.sample.SampleProbabilityFn;
import mil.nga.giat.geowave.analytic.sample.function.SamplingRankFunction;

public class SampleParameters
{
	public enum Sample
			implements
			ParameterEnum {
		SAMPLE_SIZE(
				Integer.class,
				"sss",
				"Sample Size",
				false,
				true),
		MIN_SAMPLE_SIZE(
				Integer.class,
				"sms",
				"Minimum Sample Size",
				false,
				true),
		MAX_SAMPLE_SIZE(
				Integer.class,
				"sxs",
				"Max Sample Size",
				false,
				true),
		DATA_TYPE_ID(
				String.class,
				"sdt",
				"Sample Data Type Id",
				false,
				true),
		INDEX_ID(
				String.class,
				"sdt",
				"Sample Index Type Id",
				false,
				true),
		SAMPLE_ITERATIONS(
				Integer.class,
				"ssi",
				"Minimum number of sample iterations",
				false,
				true),
		PROBABILITY_FUNCTION(
				SampleProbabilityFn.class,
				"spf",
				"The PDF determines the probability for samping an item. Used by specific sample rank functions, such as CentroidDistanceBasedSamplingRankFunction.",
				true,
				true),
		SAMPLE_RANK_FUNCTION(
				SamplingRankFunction.class,
				"srf",
				"The rank function used when sampling the first N highest rank items.",
				true,
				true);

		private transient final ParameterHelper<?> helper;

		private Sample(
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
		public ParameterHelper<?> getHelper() {
			return helper;
		}
	}

}
