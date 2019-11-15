/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.function;

/**
 * The built-in set of functions used by the GeoWave query language.
 */
public class DefaultFunctionSet implements QLFunctionRegistrySpi {

  @Override
  public QLFunctionNameAndConstructor[] getSupportedPersistables() {
    return new QLFunctionNameAndConstructor[] {
        new QLFunctionNameAndConstructor("COUNT", CountFunction::new),
        new QLFunctionNameAndConstructor("BBOX", BboxFunction::new),
        new QLFunctionNameAndConstructor("MIN", MinFunction::new),
        new QLFunctionNameAndConstructor("MAX", MaxFunction::new),
        new QLFunctionNameAndConstructor("SUM", SumFunction::new)};
  }

}
