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
package mil.nga.giat.geowave.core.cli.operations;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class ExplainCommandTest
{

	@Test
	public void testPrepare() {
		String[] args = {
			"explain"
		};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(
				registry);
		CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);

		ExplainCommand expcommand = new ExplainCommand();
		expcommand.prepare(params);
		assertEquals(
				false,
				params.isValidate());
		assertEquals(
				true,
				params.isAllowUnknown());
	}
}
