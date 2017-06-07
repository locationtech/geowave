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

import java.util.Collection;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.hadoop.conf.Configuration;

public interface FormatConfiguration
{

	public void setup(
			PropertyManagement runTimeProperties,
			Configuration configuration )
			throws Exception;

	public Class<?> getFormatClass();

	/**
	 * If the format supports only one option, then 'setting' the data has no
	 * effect.
	 * 
	 * @return true if the data is a Hadoop Writable or an POJO.
	 * 
	 */

	public boolean isDataWritable();

	public void setDataIsWritable(
			boolean isWritable );

	public Collection<ParameterEnum<?>> getParameters();
}
