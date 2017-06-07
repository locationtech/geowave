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

public class GlobalParameters
{
	public enum Global
			implements
			ParameterEnum<Object> {
		PARENT_BATCH_ID(
				String.class,
				"pb",
				"Batch ID",
				true),
		CRS_ID(
				String.class,
				"crs",
				"CRS ID",
				true),
		BATCH_ID(
				String.class,
				"b",
				"Batch ID",
				true);
		private final ParameterHelper<Object> helper;

		private Global(
				final Class baseClass,
				final String name,
				final String description,
				final boolean hasArg ) {
			helper = new BasicParameterHelper(
					this,
					baseClass,
					name,
					description,
					false,
					hasArg);
		}

		@Override
		public Enum<?> self() {
			return this;
		}

		@Override
		public ParameterHelper<Object> getHelper() {
			return helper;
		}
	}
}
