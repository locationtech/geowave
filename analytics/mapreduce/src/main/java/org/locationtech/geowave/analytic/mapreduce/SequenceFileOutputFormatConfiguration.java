/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.param.FormatConfiguration;
import org.locationtech.geowave.analytic.param.OutputParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;

public class SequenceFileOutputFormatConfiguration implements FormatConfiguration {

  final Path outputPath;

  public SequenceFileOutputFormatConfiguration() {
    outputPath = null;
  }

  public SequenceFileOutputFormatConfiguration(final Path outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public void setup(final PropertyManagement runTimeProperties, final Configuration configuration)
      throws Exception {

    final Path localOutputPath =
        outputPath == null
            ? runTimeProperties.getPropertyAsPath(OutputParameters.Output.HDFS_OUTPUT_PATH)
            : outputPath;
    if (localOutputPath != null) {
      configuration.set("mapred.output.dir", localOutputPath.toString());
    }
  }

  @Override
  public Class<?> getFormatClass() {
    return SequenceFileOutputFormat.class;
  }

  @Override
  public boolean isDataWritable() {
    return true;
  }

  @Override
  public void setDataIsWritable(final boolean isWritable) {}

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    return Arrays.asList(new ParameterEnum<?>[] {OutputParameters.Output.HDFS_OUTPUT_PATH});
  }
}
