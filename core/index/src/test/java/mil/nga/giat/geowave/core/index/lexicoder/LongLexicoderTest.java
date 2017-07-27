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

public class LongLexicoderTest extends
		AbstractLexicoderTest<Long>
{
	public LongLexicoderTest() {
		super(
				Lexicoders.LONG,
				Long.MIN_VALUE,
				Long.MAX_VALUE,
				new Long[] {
					-10l,
					Long.MIN_VALUE,
					2678l,
					Long.MAX_VALUE,
					0l
				},
				UnsignedBytes.lexicographicalComparator());
	}
}
