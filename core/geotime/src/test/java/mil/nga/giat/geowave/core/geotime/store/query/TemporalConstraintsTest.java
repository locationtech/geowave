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
package mil.nga.giat.geowave.core.geotime.store.query;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;

import org.junit.Test;

public class TemporalConstraintsTest
{

	@Test
	public void test() {
		TemporalConstraints constraints = new TemporalConstraints();
		constraints.add(new TemporalRange(
				new Date(
						1000),
				new Date(
						100002)));
		byte[] b = constraints.toBinary();

		TemporalConstraints constraintsDup = new TemporalConstraints();
		constraintsDup.fromBinary(b);

		assertEquals(
				constraints,
				constraintsDup);
	}

}
