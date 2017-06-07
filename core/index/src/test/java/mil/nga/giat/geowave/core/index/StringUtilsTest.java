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
package mil.nga.giat.geowave.core.index;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class StringUtilsTest
{
	@Test
	public void testFull() {
		String[] result = StringUtils.stringsFromBinary(StringUtils.stringsToBinary(new String[] {
			"12",
			"34"
		}));
		assertEquals(
				2,
				result.length);
		assertEquals(
				"12",
				result[0]);
		assertEquals(
				"34",
				result[1]);
	}

	@Test
	public void testEmpty() {
		String[] result = StringUtils.stringsFromBinary(StringUtils.stringsToBinary(new String[] {}));
		assertEquals(
				0,
				result.length);
	}
}
