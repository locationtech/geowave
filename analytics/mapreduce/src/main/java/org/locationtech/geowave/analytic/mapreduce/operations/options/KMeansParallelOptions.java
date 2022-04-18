/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.operations.options;

import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.param.annotations.SampleParameter;
import com.beust.jcommander.Parameter;

public class KMeansParallelOptions {
  @SampleParameter(SampleParameters.Sample.MAX_SAMPLE_SIZE)
  @Parameter(
      names = {"-sxs", "--sampleMaxSampleSize"},
      required = true,
      description = "Max Sample Size")
  private String sampleMaxSampleSize;

  @SampleParameter(SampleParameters.Sample.MIN_SAMPLE_SIZE)
  @Parameter(
      names = {"-sms", "--sampleMinSampleSize"},
      required = true,
      description = "Minimum Sample Size")
  private String sampleMinSampleSize;

  @SampleParameter(SampleParameters.Sample.SAMPLE_ITERATIONS)
  @Parameter(
      names = {"-ssi", "--sampleSampleIterations"},
      required = true,
      description = "Minimum number of sample iterations")
  private String sampleSampleIterations;

  public String getSampleMaxSampleSize() {
    return sampleMaxSampleSize;
  }

  public void setSampleMaxSampleSize(final String sampleMaxSampleSize) {
    this.sampleMaxSampleSize = sampleMaxSampleSize;
  }

  public String getSampleMinSampleSize() {
    return sampleMinSampleSize;
  }

  public void setSampleMinSampleSize(final String sampleMinSampleSize) {
    this.sampleMinSampleSize = sampleMinSampleSize;
  }

  public String getSampleSampleIterations() {
    return sampleSampleIterations;
  }

  public void setSampleSampleIterations(final String sampleSampleIterations) {
    this.sampleSampleIterations = sampleSampleIterations;
  }
}
