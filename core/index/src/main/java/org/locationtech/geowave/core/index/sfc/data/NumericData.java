/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.sfc.data;

import org.locationtech.geowave.core.index.persist.Persistable;

/** Interface used to define numeric data associated with a space filling curve. */
public interface NumericData extends java.io.Serializable, Persistable {
  public double getMin();

  public double getMax();

  public double getCentroid();

  public boolean isRange();
}
