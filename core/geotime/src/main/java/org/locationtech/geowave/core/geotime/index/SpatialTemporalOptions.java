/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index;

import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider.Bias;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider.BiasConverter;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider.UnitConverter;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import com.beust.jcommander.Parameter;

public class SpatialTemporalOptions extends CommonSpatialOptions {
  protected static Unit DEFAULT_PERIODICITY = Unit.YEAR;

  @Parameter(
      names = {"--period"},
      required = false,
      description = "The periodicity of the temporal dimension.  Because time is continuous, it is binned at this interval.",
      converter = UnitConverter.class)
  protected Unit periodicity = DEFAULT_PERIODICITY;

  @Parameter(
      names = {"--bias"},
      required = false,
      description = "The bias of the spatial-temporal index. There can be more precision given to time or space if necessary.",
      converter = BiasConverter.class)
  protected Bias bias = Bias.BALANCED;

  @Parameter(
      names = {"--maxDuplicates"},
      required = false,
      description = "The max number of duplicates per dimension range.  The default is 2 per range (for example lines and polygon timestamp data would be up to 4 because its 2 dimensions, and line/poly time range data would be 8).")
  protected long maxDuplicates = -1;

  public void setPeriodicity(final Unit periodicity) {
    this.periodicity = periodicity;
  }

  public Unit getPeriodicity() {
    return periodicity;
  }

  public void setBias(final Bias bias) {
    this.bias = bias;
  }

  public Bias getBias() {
    return bias;
  }

  public void setMaxDuplicates(final long maxDuplicates) {
    this.maxDuplicates = maxDuplicates;
  }

  public long getMaxDuplicates() {
    return maxDuplicates;
  }
}
