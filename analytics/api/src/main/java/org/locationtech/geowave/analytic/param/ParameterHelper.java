/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.param;

import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.analytic.PropertyManagement;

public interface ParameterHelper<T> extends Serializable {
  public Class<T> getBaseClass();

  public T getValue(PropertyManagement propertyManagement);

  public void setValue(PropertyManagement propertyManagement, T value);

  public void setValue(Configuration config, Class<?> scope, T value);

  public T getValue(JobContext context, Class<?> scope, T defaultValue);
}
