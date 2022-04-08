/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.lexicoder;

import com.google.common.primitives.UnsignedBytes;

public class ByteLexicoderTest extends AbstractLexicoderTest<Byte> {
  public ByteLexicoderTest() {
    super(
        Lexicoders.BYTE,
        Byte.MIN_VALUE,
        Byte.MAX_VALUE,
        new Byte[] {
            (byte) -10,
            Byte.MIN_VALUE,
            (byte) 11,
            (byte) -122,
            (byte) 122,
            (byte) -100,
            (byte) 100,
            Byte.MAX_VALUE,
            (byte) 0},
        UnsignedBytes.lexicographicalComparator());
  }
}
