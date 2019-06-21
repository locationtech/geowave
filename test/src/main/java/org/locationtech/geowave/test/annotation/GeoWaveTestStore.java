/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.locationtech.geowave.test.AccumuloStoreTestEnvironment;
import org.locationtech.geowave.test.BigtableStoreTestEnvironment;
import org.locationtech.geowave.test.CassandraStoreTestEnvironment;
import org.locationtech.geowave.test.DynamoDBStoreTestEnvironment;
import org.locationtech.geowave.test.HBaseStoreTestEnvironment;
import org.locationtech.geowave.test.KuduStoreTestEnvironment;
import org.locationtech.geowave.test.RedisStoreTestEnvironment;
import org.locationtech.geowave.test.RocksDBStoreTestEnvironment;
import org.locationtech.geowave.test.StoreTestEnvironment;
import org.locationtech.geowave.test.TestUtils;

/**
 * The <code>DataStores</code> annotation specifies the GeoWave DataStore to be run when a class
 * annotated with <code>@RunWith(GeoWaveIT.class)</code> is run.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface GeoWaveTestStore {
  /** @return the data stores to run with */
  public GeoWaveStoreType[] value();

  /** @return the namespace to associate the store with */
  public String namespace() default TestUtils.TEST_NAMESPACE;

  /**
   * @return a "key=value" pair that will override default options for the client-side configuration
   *         of this datastore
   */
  public String[] options() default "";

  public static enum GeoWaveStoreType {
    DYNAMODB(DynamoDBStoreTestEnvironment.getInstance()),
    ACCUMULO(AccumuloStoreTestEnvironment.getInstance()),
    BIGTABLE(BigtableStoreTestEnvironment.getInstance()),
    CASSANDRA(CassandraStoreTestEnvironment.getInstance()),
    HBASE(HBaseStoreTestEnvironment.getInstance()),
    KUDU(KuduStoreTestEnvironment.getInstance()),
    REDIS(RedisStoreTestEnvironment.getInstance()),
    ROCKSDB(RocksDBStoreTestEnvironment.getInstance());
    private final StoreTestEnvironment testEnvironment;

    private GeoWaveStoreType(final StoreTestEnvironment testEnvironment) {
      this.testEnvironment = testEnvironment;
    }

    public StoreTestEnvironment getTestEnvironment() {
      return testEnvironment;
    }
  }
}
