/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.config;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.kudu.KuduStoreFactoryFamily;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class KuduRequiredOptions extends StoreFactoryOptions {
  @Parameter(
      names = "--kuduMaster",
      required = true,
      description = "A URL for the Kudu master node")
  private String kuduMaster;

  @ParametersDelegate
  private KuduOptions additionalOptions = new KuduOptions();

  public KuduRequiredOptions() {}

  public KuduRequiredOptions(
      final String kuduMaster,
      final String gwNamespace,
      final KuduOptions additionalOptions) {
    super(gwNamespace);
    this.kuduMaster = kuduMaster;
    this.additionalOptions = additionalOptions;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new KuduStoreFactoryFamily();
  }

  public String getKuduMaster() {
    return kuduMaster;
  }

  public void setKuduMaster(final String kuduMaster) {
    this.kuduMaster = kuduMaster;
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return additionalOptions;
  }

}
