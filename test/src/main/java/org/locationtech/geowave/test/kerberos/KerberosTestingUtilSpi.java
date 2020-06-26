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
