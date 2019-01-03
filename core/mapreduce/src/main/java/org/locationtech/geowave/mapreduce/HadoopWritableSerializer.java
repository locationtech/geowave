/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce;

import org.apache.hadoop.io.Writable;

/**
 * @param <T> the native type
 * @param <W> the writable type
 */
public interface HadoopWritableSerializer<T, W extends Writable> {
  public W toWritable(T entry);

  public T fromWritable(W writable);
}
