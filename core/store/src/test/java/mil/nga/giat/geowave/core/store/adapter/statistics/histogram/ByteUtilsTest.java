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
package mil.nga.giat.geowave.core.store.adapter.statistics.histogram;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class ByteUtilsTest
{
	@Test
	public void test() {

		double oneTwo = ByteUtils.toDouble("12".getBytes());
		double oneOneTwo = ByteUtils.toDouble("112".getBytes());
		double oneThree = ByteUtils.toDouble("13".getBytes());
		double oneOneThree = ByteUtils.toDouble("113".getBytes());
		assertTrue(oneTwo > oneOneTwo);
		assertTrue(oneThree > oneTwo);
		assertTrue(oneOneTwo < oneOneThree);
		assertTrue(Arrays.equals(
				ByteUtils.toPaddedBytes("113".getBytes()),
				ByteUtils.toBytes(oneOneThree)));
	}
}
