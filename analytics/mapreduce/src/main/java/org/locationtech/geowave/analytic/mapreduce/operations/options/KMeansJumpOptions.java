/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.operations.options;

import org.locationtech.geowave.analytic.param.JumpParameters;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.param.annotations.JumpParameter;
import org.locationtech.geowave.analytic.param.annotations.SampleParameter;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class KMeansJumpOptions {

  @JumpParameter(JumpParameters.Jump.KPLUSPLUS_MIN)
  @Parameter(
      names = {"-jkp", "--jumpKplusplusMin"},
      required = true,
      description = "The minimum k when K means ++ takes over sampling.")
  private String jumpKplusplusMin;

  @JumpParameter(JumpParameters.Jump.RANGE_OF_CENTROIDS)
  @Parameter(
      names = {"-jrc", "--jumpRangeOfCentroids"},
      required = true,
      description = "Comma-separated range of centroids (e.g. 2,100)",
      converter = NumericRangeConverter.class)
  private NumericRange jumpRangeOfCentroids;

  @SampleParameter(SampleParameters.Sample.SAMPLE_RANK_FUNCTION)
  @Parameter(
      names = {"-srf", "--sampleSampleRankFunction"},
      hidden = true,
      description = "The rank function used when sampling the first N highest rank items.")
  private String sampleSampleRankFunction;

  @SampleParameter(SampleParameters.Sample.SAMPLE_SIZE)
  @Parameter(names = {"-sss", "--sampleSampleSize"}, hidden = true, description = "Sample Size")
  private String sampleSampleSize;

  public String getJumpKplusplusMin() {
    return jumpKplusplusMin;
  }

  public void setJumpKplusplusMin(final String jumpKplusplusMin) {
    this.jumpKplusplusMin = jumpKplusplusMin;
  }

  public NumericRange getJumpRangeOfCentroids() {
    return jumpRangeOfCentroids;
  }

  public void setJumpRangeOfCentroids(final NumericRange jumpRangeOfCentroids) {
    this.jumpRangeOfCentroids = jumpRangeOfCentroids;
  }

  public String getSampleSampleRankFunction() {
    return sampleSampleRankFunction;
  }

  public void setSampleSampleRankFunction(final String sampleSampleRankFunction) {
    this.sampleSampleRankFunction = sampleSampleRankFunction;
  }

  public String getSampleSampleSize() {
    return sampleSampleSize;
  }

  public void setSampleSampleSize(final String sampleSampleSize) {
    this.sampleSampleSize = sampleSampleSize;
  }

  public static class NumericRangeConverter implements IStringConverter<NumericRange> {

    @Override
    public NumericRange convert(final String value) {
      final String p = value.toString();
      final String[] parts = p.split(",");
      try {
        if (parts.length == 2) {
          return new NumericRange(
              Double.parseDouble(parts[0].trim()),
              Double.parseDouble(parts[1].trim()));
        } else {
          return new NumericRange(0, Double.parseDouble(p));
        }
      } catch (final Exception ex) {
        throw new ParameterException("Invalid range parameter " + value, ex);
      }
    }
  }
}
