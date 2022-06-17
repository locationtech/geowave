/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.test.TestEnvironment;
import org.locationtech.geowave.test.kafka.KafkaTestEnvironment;
import org.locationtech.geowave.test.mapreduce.MapReduceTestEnvironment;
import org.locationtech.geowave.test.services.ServicesTestEnvironment;
import org.locationtech.geowave.test.spark.SparkTestEnvironment;

/**
 * The <code>Environments</code> annotation specifies the GeoWave test environments to be setup and
 * torn down when a class annotated with <code>@RunWith(GeoWaveIT.class)</code> is run.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Environments {
  /** @return the data stores to run with */
  public Environment[] value();

  public static enum Environment {
    MAP_REDUCE(MapReduceTestEnvironment.getInstance()),
    KAFKA(KafkaTestEnvironment.getInstance()),
    SERVICES(ServicesTestEnvironment.getInstance()),
    SPARK(SparkTestEnvironment.getInstance());

    private final TestEnvironment testEnvironment;

    private Environment(final TestEnvironment testEnvironment) {
      this.testEnvironment = testEnvironment;
    }

    public TestEnvironment getTestEnvironment() {
      return testEnvironment;
    }
  }
}
