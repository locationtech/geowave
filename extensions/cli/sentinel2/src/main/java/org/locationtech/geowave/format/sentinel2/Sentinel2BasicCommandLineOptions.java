/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.sentinel2;

import java.util.Date;
import org.locationtech.geowave.adapter.vector.ingest.CQLFilterOptionProvider.ConvertCQLStrToFilterConverter;
import org.locationtech.geowave.adapter.vector.ingest.CQLFilterOptionProvider.FilterParameter;
import org.opengis.filter.Filter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.ISO8601DateConverter;
import com.beust.jcommander.converters.IntegerConverter;

public class Sentinel2BasicCommandLineOptions {
  private static final String DEFAULT_WORKSPACE_DIR = "sentinel2";

  @Parameter(
      names = {"-ws", "--workspaceDir"},
      description = "A local directory to write temporary files needed for Sentinel2 ingest. Default is <TEMP_DIR>/sentinel2")
  private String workspaceDir = DEFAULT_WORKSPACE_DIR;

  @Parameter(names = "--provider", description = "Name of Sentinel2 provider ('THEIA','AWS').")
  private String providerName = "THEIA";

  @Parameter(
      names = "--collection",
      description = "Product collection to fetch within Sentinel2 collections ('SENTINEL2').")
  private String collection = "SENTINEL2";

  @Parameter(names = "--platform", description = "Satellite ('SENTINEL2A','SENTINEL2B',...).")
  private String platform = "";

  @Parameter(
      names = "--location",
      description = "Product location, 100 km Grid Square ID of the Military Grid Reference System (EX: 'T30TWM').")
  private String location = "";

  @Parameter(
      names = {"-s", "--startdate"},
      description = "Optional start Date filter.",
      converter = ISO8601DateConverter.class)
  private Date startDate;

  @Parameter(
      names = {"-f", "--enddate"},
      description = "Optional end Date filter.",
      converter = ISO8601DateConverter.class)
  private Date endDate;

  @Parameter(
      names = "--orbitnumber",
      description = "Optional Orbit Number filter.",
      converter = IntegerConverter.class)
  private int orbitNumber = 0;

  @Parameter(
      names = "--relativeorbitnumber",
      description = "Optional Relative Orbit Number filter.",
      converter = IntegerConverter.class)
  private int relativeOrbitNumber = 0;

  @Parameter(
      names = "--cql",
      description = "An optional CQL expression to filter the ingested imagery. The feature type for the expression has the following attributes: shape (Geometry), location (String), productIdentifier (String), productType (String), collection (String), platform (String), processingLevel (String), startDate (Date), quicklook (String), thumbnail (String), bands (String), resolution (int), cloudCover (int), snowCover (int), waterCover (int), orbitNumber (int), relativeOrbitNumber (int) and the feature ID is entityId for the scene.  Additionally attributes of the individuals band can be used such as band (String).",
      converter = ConvertCQLStrToFilterConverter.class)
  private FilterParameter cqlFilter = new FilterParameter(null, null);

  public Sentinel2BasicCommandLineOptions() {}

  public String getWorkspaceDir() {
    return workspaceDir;
  }

  public Filter getCqlFilter() {
    if (cqlFilter != null) {
      return cqlFilter.getFilter();
    }
    return null;
  }

  public String providerName() {
    return providerName;
  }

  public String collection() {
    return collection;
  }

  public String platform() {
    return platform;
  }

  public String location() {
    return location;
  }

  public Date startDate() {
    return startDate;
  }

  public Date endDate() {
    return endDate;
  }

  public int orbitNumber() {
    return orbitNumber;
  }

  public int relativeOrbitNumber() {
    return relativeOrbitNumber;
  }

  public void setWorkspaceDir(final String workspaceDir) {
    this.workspaceDir = workspaceDir;
  }

  public void setCqlFilter(final String cqlFilter) {
    this.cqlFilter = new ConvertCQLStrToFilterConverter().convert(cqlFilter);
  }

  public void setProviderName(final String providerName) {
    this.providerName = providerName;
  }

  public void setCollection(final String collection) {
    this.collection = collection;
  }

  public void setPlatform(final String platform) {
    this.platform = platform;
  }

  public void setLocation(final String location) {
    this.location = location;
  }

  public void setStartDate(final Date startDate) {
    this.startDate = startDate;
  }

  public void setEndDate(final Date endDate) {
    this.endDate = endDate;
  }

  public void setOrbitNumber(final int orbitNumber) {
    this.orbitNumber = orbitNumber;
  }

  public void setRelativeOrbitNumber(final int relativeOrbitNumber) {
    this.relativeOrbitNumber = relativeOrbitNumber;
  }
}
