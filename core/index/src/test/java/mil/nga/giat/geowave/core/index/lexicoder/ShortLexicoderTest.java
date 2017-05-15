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

public class ShortLexicoderTest extends
		AbstractLexicoderTest<Short>
{
	public ShortLexicoderTest() {
		super(
				Lexicoders.SHORT,
				Short.MIN_VALUE,
				Short.MAX_VALUE,
				new Short[] {
					(short) -10,
					Short.MIN_VALUE,
					(short) 2678,
					Short.MAX_VALUE,
					(short) 0
				},
				UnsignedBytes.lexicographicalComparator());
	}
}
