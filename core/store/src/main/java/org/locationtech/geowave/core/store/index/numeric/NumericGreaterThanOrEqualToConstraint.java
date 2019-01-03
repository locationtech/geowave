/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index.numeric;

public class NumericGreaterThanOrEqualToConstraint extends NumericQueryConstraint {

  public NumericGreaterThanOrEqualToConstraint(final String fieldName, final Number number) {
    super(fieldName, number, Double.MAX_VALUE, true, true);
  }
}
