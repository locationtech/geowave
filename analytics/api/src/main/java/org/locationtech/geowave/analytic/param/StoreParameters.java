/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.param;

public class StoreParameters {
  public enum StoreParam implements ParameterEnum {
    INPUT_STORE(new InputStoreParameterHelper()), OUTPUT_STORE(new OutputStoreParameterHelper()),;

    private final ParameterHelper<?> helper;

    private StoreParam(final ParameterHelper<?> helper) {
      this.helper = helper;
    }

    @Override
    public Enum<?> self() {
      return this;
    }

    @Override
    public ParameterHelper<?> getHelper() {
      return helper;
    }
  }
}
