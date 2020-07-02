/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.kerberos;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;

/**
 * The only reason this exists is to decouple a compile-time requirement for an implementation
 * considering older versions and cloudera versions of accumulo do not have TestingKDC
 */
public interface KerberosTestingUtilSpi {
  void setup() throws Exception;

  void tearDown() throws Exception;

  void configureMiniAccumulo(MiniAccumuloConfigImpl cfg, Configuration coreSite);

  ClusterUser getRootUser();
}
