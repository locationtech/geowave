/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.nn;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import org.locationtech.geowave.analytic.mapreduce.MapReduceJobController;
import org.locationtech.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;

public class GeoWaveExtractNNJobRunner extends NNJobRunner {

  public GeoWaveExtractNNJobRunner() {
    super();
    setInputFormatConfiguration(new GeoWaveInputFormatConfiguration());
    setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration());
    super.setReducerCount(4);
  }

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    final Set<ParameterEnum<?>> params = new HashSet<>();
    params.addAll(super.getParameters());
    params.addAll(MapReduceParameters.getParameters());
    return params;
  }

  @Override
  public int run(final PropertyManagement runTimeProperties) throws Exception {
    return this.run(MapReduceJobController.getConfiguration(runTimeProperties), runTimeProperties);
  }
}
