/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.config;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.cassandra.CassandraStoreFactoryFamily;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class CassandraRequiredOptions extends StoreFactoryOptions {
  @Parameter(
      names = "--contactPoints",
      description = "A single contact point or a comma delimited set of contact points to connect to the Cassandra cluster.")
  private String contactPoints = "";
  @Parameter(names = "--datacenter", description = "The local datacenter.")
  private String datacenter = null;

  @ParametersDelegate
  private CassandraOptions additionalOptions = new CassandraOptions();

  public CassandraRequiredOptions() {}

  public CassandraRequiredOptions(
      final String contactPoints,
      final String gwNamespace,
      final CassandraOptions additionalOptions) {
    super(gwNamespace);
    this.contactPoints = contactPoints;
    this.additionalOptions = additionalOptions;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new CassandraStoreFactoryFamily();
  }

  public String getContactPoints() {
    return contactPoints;
  }

  public void setContactPoints(final String contactPoints) {
    this.contactPoints = contactPoints;
  }

  public String getDatacenter() {
    return datacenter;
  }

  public void setDatacenter(String datacenter) {
    this.datacenter = datacenter;
  }

  public CassandraOptions getAdditionalOptions() {
    return additionalOptions;
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return additionalOptions;
  }
}
