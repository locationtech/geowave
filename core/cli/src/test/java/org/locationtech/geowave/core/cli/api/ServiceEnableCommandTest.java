/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.cli.api;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;

public class ServiceEnableCommandTest
{

	private class ServiceEnabledCommand_TESTING extends
			ServiceEnabledCommand
	{

		private HttpMethod method;

		public ServiceEnabledCommand_TESTING(
				HttpMethod method ) {
			this.method = method;
		}

		@Override
		public void execute(
				OperationParams params )
				throws Exception {}

		@Override
		public Object computeResults(
				OperationParams params )
				throws Exception {
			return null;
		}

		@Override
		public HttpMethod getMethod() {
			return method;
		}

	}

	@Before
	public void setUp()
			throws Exception {}

	@After
	public void tearDown()
			throws Exception {}

	@Test
	public void defaultSuccessStatusIs200ForGET() {

		ServiceEnabledCommand_TESTING classUnderTest = new ServiceEnabledCommand_TESTING(
				HttpMethod.GET);

		Assert.assertEquals(
				true,
				classUnderTest.successStatusIs200());
	}

	@Test
	public void defaultSuccessStatusIs201ForPOST() {

		ServiceEnabledCommand_TESTING classUnderTest = new ServiceEnabledCommand_TESTING(
				HttpMethod.POST);

		Assert.assertEquals(
				false,
				classUnderTest.successStatusIs200());
	}

}
