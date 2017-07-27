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
package mil.nga.giat.geowave.core.cli.spi;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.ExplainCommand;

public class OperationRegistryTest
{

	@Test
	public void testGetOperation() {
		OperationEntry optentry = new OperationEntry(
				ExplainCommand.class);
		List<OperationEntry> entries = new ArrayList<OperationEntry>();
		entries.add(optentry);
		OperationRegistry optreg = new OperationRegistry(
				entries);

		assertEquals(
				"explain",
				optreg.getOperation(
						ExplainCommand.class).getOperationName());
		assertEquals(
				true,
				optreg.getAllOperations().contains(
						optentry));
	}

}
