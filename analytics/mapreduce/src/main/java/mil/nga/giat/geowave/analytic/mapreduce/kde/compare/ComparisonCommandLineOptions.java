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
package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import com.beust.jcommander.Parameter;

public class ComparisonCommandLineOptions
{
	@Parameter(names = "--timeAttribute", description = "The name of the time attribute")
	private String timeAttribute;

	public ComparisonCommandLineOptions() {

	}

	public ComparisonCommandLineOptions(
			final String timeAttribute ) {
		this.timeAttribute = timeAttribute;
	}

	public String getTimeAttribute() {
		return timeAttribute;
	}
}
