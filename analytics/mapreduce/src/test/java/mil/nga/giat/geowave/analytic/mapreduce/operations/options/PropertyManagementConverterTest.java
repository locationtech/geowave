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
package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;

public class PropertyManagementConverterTest
{

	@Test
	public void testConverter()
			throws Exception {
		PropertyManagement propMgmt = new PropertyManagement();
		PropertyManagementConverter conv = new PropertyManagementConverter(
				propMgmt);

		DBScanOptions opts = new DBScanOptions();
		opts.setGlobalBatchId("some-value");

		conv.readProperties(opts);

		Assert.assertEquals(
				"some-value",
				propMgmt.getProperty(GlobalParameters.Global.BATCH_ID));

	}
}
