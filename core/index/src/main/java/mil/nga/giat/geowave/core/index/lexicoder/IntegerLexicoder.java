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
package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.primitives.Ints;

/**
 * A lexicoder for signed integers (in the range from Integer.MIN_VALUE to
 * Integer.MAX_VALUE). Does an exclusive or on the most significant bit to
 * invert the sign, so that lexicographic ordering of the byte arrays matches
 * the natural order of the numbers.
 * 
 * See Apache Accumulo
 * (org.apache.accumulo.core.client.lexicoder.IntegerLexicoder)
 */
public class IntegerLexicoder implements
		NumberLexicoder<Integer>
{

	protected IntegerLexicoder() {}

	@Override
	public byte[] toByteArray(
			final Integer value ) {
		return Ints.toByteArray(value ^ 0x80000000);
	}

	@Override
	public Integer fromByteArray(
			final byte[] bytes ) {
		final int value = Ints.fromByteArray(bytes);
		return value ^ 0x80000000;
	}

	@Override
	public Integer getMinimumValue() {
		return Integer.MIN_VALUE;
	}

	@Override
	public Integer getMaximumValue() {
		return Integer.MAX_VALUE;
	}

}
