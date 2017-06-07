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

import com.google.common.primitives.UnsignedBytes;

public class IntegerLexicoderTest extends
		AbstractLexicoderTest<Integer>
{
	public IntegerLexicoderTest() {
		super(
				Lexicoders.INT,
				Integer.MIN_VALUE,
				Integer.MAX_VALUE,
				new Integer[] {
					-10,
					Integer.MIN_VALUE,
					2678,
					Integer.MAX_VALUE,
					0
				},
				UnsignedBytes.lexicographicalComparator());
	}
}
