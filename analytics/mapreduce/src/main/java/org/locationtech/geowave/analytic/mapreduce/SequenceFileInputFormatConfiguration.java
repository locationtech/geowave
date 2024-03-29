/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce;

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.param.FormatConfiguration;
import org.locationtech.geowave.analytic.param.InputParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;

public class SequenceFileInputFormatConfiguration implements FormatConfiguration {

  final Path inputPath;

  public SequenceFileInputFormatConfiguration() {
    inputPath = null;
  }

  public SequenceFileInputFormatConfiguration(final Path inputPath) {
    this.inputPath = inputPath;
  }

  @Override
  public void setup(final PropertyManagement runTimeProperties, final Configuration configuration)
      throws Exception {
    final Path localInputPath =
        inputPath == null
            ? runTimeProperties.getPropertyAsPath(InputParameters.Input.HDFS_INPUT_PATH)
            : inputPath;
    if (localInputPath != null) {
      configuration.set("mapred.input.dir", localInputPath.toString());
    }
  }

  @Override
  public Class<?> getFormatClass() {
    return SequenceFileInputFormat.class;
  }

  @Override
  public boolean isDataWritable() {
    return true;
  }

  @Override
  public void setDataIsWritable(final boolean isWritable) {}

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    return Arrays.asList(new ParameterEnum<?>[] {InputParameters.Input.HDFS_INPUT_PATH});
  }
}
