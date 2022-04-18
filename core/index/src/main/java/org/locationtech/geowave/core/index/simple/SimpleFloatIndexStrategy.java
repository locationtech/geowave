/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.simple;

import org.locationtech.geowave.core.index.lexicoder.Lexicoders;

public class SimpleFloatIndexStrategy extends SimpleNumericIndexStrategy<Float> {

  public SimpleFloatIndexStrategy() {
    super(Lexicoders.FLOAT);
  }

  @Override
  protected Float cast(final double value) {
    return (float) value;
  }

  @Override
  protected boolean isInteger() {
    return false;
  }
}
