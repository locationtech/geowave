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

public class FloatLexicoderTest extends AbstractLexicoderTest<Float> {
  public FloatLexicoderTest() {
    super(
        Lexicoders.FLOAT,
        -Float.MAX_VALUE,
        Float.MAX_VALUE,
        new Float[] {
            -10f,
            -Float.MAX_VALUE,
            11f,
            -14.2f,
            14.2f,
            -100.002f,
            100.002f,
            -11f,
            Float.MAX_VALUE,
            0f},
        UnsignedBytes.lexicographicalComparator());
  }
}
