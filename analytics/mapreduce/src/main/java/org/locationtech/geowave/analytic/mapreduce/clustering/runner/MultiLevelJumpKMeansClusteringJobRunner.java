/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.clustering.runner;

import org.locationtech.geowave.analytic.mapreduce.kmeans.runner.KMeansJumpJobRunner;

/** @see KMeansJumpJobRunner */
public class MultiLevelJumpKMeansClusteringJobRunner extends MultiLevelClusteringJobRunner {

  @Override
  protected ClusteringRunner getClusteringRunner() {
    return new KMeansJumpJobRunner();
  }
}
