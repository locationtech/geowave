/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.clustering.runner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import org.locationtech.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import org.locationtech.geowave.analytic.mapreduce.clustering.GroupAssignmentMapReduce;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.MapReduceParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.StoreParameters;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;

/** Assign group IDs to input items based on centroids. */
public class GroupAssigmentJobRunner extends GeoWaveAnalyticJobRunner {
  private int zoomLevel = 1;

  public GroupAssigmentJobRunner() {
    super.setReducerCount(8);
  }

  public void setZoomLevel(final int zoomLevel) {
    this.zoomLevel = zoomLevel;
  }

  @Override
  public void configure(final Job job) throws Exception {
    job.setMapperClass(GroupAssignmentMapReduce.GroupAssignmentMapper.class);
    job.setMapOutputKeyClass(GeoWaveInputKey.class);
    job.setMapOutputValueClass(ObjectWritable.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(GeoWaveInputKey.class);
    job.setOutputValueClass(ObjectWritable.class);
  }

  @Override
  public Class<?> getScope() {
    return GroupAssignmentMapReduce.class;
  }

  @Override
  public int run(final Configuration config, final PropertyManagement runTimeProperties)
      throws Exception {

    // Required since the Mapper uses the input format parameters to lookup
    // the adapter
    final DataStorePluginOptions dataStoreOptions =
        ((PersistableStore) runTimeProperties.getProperty(
            StoreParam.INPUT_STORE)).getDataStoreOptions();
    GeoWaveInputFormat.setStoreOptions(config, dataStoreOptions);
    runTimeProperties.setConfig(
        new ParameterEnum[] {
            CentroidParameters.Centroid.EXTRACTOR_CLASS,
            CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,},
        config,
        GroupAssignmentMapReduce.class);
    NestedGroupCentroidAssignment.setParameters(config, getScope(), runTimeProperties);
    CentroidManagerGeoWave.setParameters(config, getScope(), runTimeProperties);

    NestedGroupCentroidAssignment.setZoomLevel(config, getScope(), zoomLevel);

    // HP Fortify "Command Injection" false positive
    // What Fortify considers "externally-influenced input"
    // comes only from users with OS-level access anyway
    return super.run(config, runTimeProperties);
  }

  @Override
  public Collection<ParameterEnum<?>> getParameters() {
    final Set<ParameterEnum<?>> params = new HashSet<>();
    params.addAll(super.getParameters());

    params.addAll(
        Arrays.asList(
            new ParameterEnum<?>[] {
                StoreParameters.StoreParam.INPUT_STORE,
                GlobalParameters.Global.BATCH_ID}));

    params.addAll(CentroidManagerGeoWave.getParameters());
    params.addAll(MapReduceParameters.getParameters());
    params.addAll(NestedGroupCentroidAssignment.getParameters());
    return params;
  }

  @Override
  protected String getJobName() {
    return "Group Assignment";
  }
}
