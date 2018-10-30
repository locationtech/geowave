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
package org.locationtech.geowave.core.index;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;

public class ByteArrayUtilsTest
{

	@Test
	public void testSplit() {
		final ByteArray first = new ByteArray(
				"first");
		final ByteArray second = new ByteArray(
				"second");
		final byte[] combined = ByteArrayUtils.combineVariableLengthArrays(
				first.getBytes(),
				second.getBytes());
		final Pair<byte[], byte[]> split = ByteArrayUtils.splitVariableLengthArrays(combined);
		Assert.assertArrayEquals(
				first.getBytes(),
				split.getLeft());
		Assert.assertArrayEquals(
				second.getBytes(),
				split.getRight());
	}
}
