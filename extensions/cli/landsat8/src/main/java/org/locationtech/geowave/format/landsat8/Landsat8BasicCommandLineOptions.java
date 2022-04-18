/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import org.locationtech.geowave.adapter.vector.ingest.CQLFilterOptionProvider.ConvertCQLStrToFilterConverter;
import org.locationtech.geowave.adapter.vector.ingest.CQLFilterOptionProvider.FilterParameter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.opengis.filter.Filter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IntegerConverter;

public class Landsat8BasicCommandLineOptions {
  private static final String DEFAULT_WORKSPACE_DIR = "landsat8";

  @Parameter(
      names = {"-ws", "--workspaceDir"},
      description = "A local directory to write temporary files needed for landsat 8 ingest. Default is <TEMP_DIR>/landsat8")
  private String workspaceDir = DEFAULT_WORKSPACE_DIR;

  @Parameter(
      names = "--cql",
      description = "An optional CQL expression to filter the ingested imagery. The feature type for the expression has the following attributes: shape (Geometry) in "
          + GeometryUtils.DEFAULT_CRS_STR
          + ", acquisitionDate (Date), cloudCover (double), processingLevel (String), path (int), row (int) and the feature ID is productId for the scene.  Additionally attributes of the individuals band can be used such as band (String), sizeMB (double), and bandDownloadUrl (String)",
      converter = ConvertCQLStrToFilterConverter.class)
  private FilterParameter cqlFilter = new FilterParameter(null, null);

  @Parameter(
      names = "--sincelastrun",
      arity = 1,
      description = "An option to check the scenes list from the workspace and if it exists, to only ingest data since the last scene.")
  private boolean onlyScenesSinceLastRun;

  @Parameter(
      names = "--usecachedscenes",
      arity = 1,
      description = "An option to run against the existing scenes catalog in the workspace directory if it exists.")
  private boolean useCachedScenes;

  @Parameter(
      names = "--nbestscenes",
      description = "An option to identify and only use a set number of scenes with the best cloud cover",
      converter = IntegerConverter.class)
  private int nBestScenes;

  @Parameter(
      names = "--nbestbands",
      description = "An option to identify and only use a set number of bands with the best cloud cover",
      converter = IntegerConverter.class)
  private int nBestBands;

  @Parameter(
      names = "--nbestperspatial",
      arity = 1,
      description = "A boolean flag, when applied with --nbestscenes or --nbestbands will aggregate scenes and/or bands by path/row")
  private boolean nBestPerSpatial;

  public Landsat8BasicCommandLineOptions() {}

  public String getWorkspaceDir() {
    return workspaceDir;
  }

  public Filter getCqlFilter() {
    if (cqlFilter != null) {
      return cqlFilter.getFilter();
    }
    return null;
  }

  public boolean isUseCachedScenes() {
    return useCachedScenes;
  }

  public boolean isOnlyScenesSinceLastRun() {
    return onlyScenesSinceLastRun;
  }

  public int getNBestScenes() {
    return nBestScenes;
  }

  public boolean isNBestPerSpatial() {
    return nBestPerSpatial;
  }

  public int getNBestBands() {
    return nBestBands;
  }

  public void setWorkspaceDir(final String workspaceDir) {
    this.workspaceDir = workspaceDir;
  }

  public void setCqlFilter(final String cqlFilter) {
    this.cqlFilter = new ConvertCQLStrToFilterConverter().convert(cqlFilter);
  }

  public void setOnlyScenesSinceLastRun(final boolean onlyScenesSinceLastRun) {
    this.onlyScenesSinceLastRun = onlyScenesSinceLastRun;
  }

  public void setUseCachedScenes(final boolean useCachedScenes) {
    this.useCachedScenes = useCachedScenes;
  }

  public void setNBestScenes(final int nBestScenes) {
    this.nBestScenes = nBestScenes;
  }

  public void setNBestBands(final int nBestBands) {
    this.nBestBands = nBestBands;
  }

  public void setNBestPerSpatial(final boolean nBestPerSpatial) {
    this.nBestPerSpatial = nBestPerSpatial;
  }
}
