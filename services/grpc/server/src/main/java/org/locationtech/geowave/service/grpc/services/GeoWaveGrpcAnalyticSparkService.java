/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.grpc.services;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.util.Map;
import org.locationtech.geowave.analytic.spark.kmeans.operations.KmeansSparkCommand;
import org.locationtech.geowave.analytic.spark.sparksql.operations.SparkSqlCommand;
import org.locationtech.geowave.analytic.spark.spatial.operations.SpatialJoinCommand;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticSparkGrpc;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.KmeansSparkCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SparkSqlCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.SpatialJoinCommandParametersProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveGrpcAnalyticSparkService extends AnalyticSparkGrpc.AnalyticSparkImplBase
    implements GeoWaveGrpcServiceSpi {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcAnalyticSparkService.class.getName());

  @Override
  public BindableService getBindableService() {
    return (BindableService) this;
  }

  @Override
  public void kmeansSparkCommand(
      KmeansSparkCommandParametersProtos request, StreamObserver<VoidResponseProtos> responseObserver) {
    KmeansSparkCommand cmd = new KmeansSparkCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);
    LOGGER.info("Executing KmeansSparkCommand...");
    try {
      cmd.computeResults(params);
      VoidResponseProtos resp = VoidResponseProtos.newBuilder().build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
    }
  }

  @Override
  public void sparkSqlCommand(
      SparkSqlCommandParametersProtos request, StreamObserver<VoidResponseProtos> responseObserver) {
    SparkSqlCommand cmd = new SparkSqlCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);
    LOGGER.info("Executing SparkSqlCommand...");
    try {
      cmd.computeResults(params);
      VoidResponseProtos resp = VoidResponseProtos.newBuilder().build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
    }
  }

  @Override
  public void spatialJoinCommand(
      SpatialJoinCommandParametersProtos request, StreamObserver<VoidResponseProtos> responseObserver) {
    SpatialJoinCommand cmd = new SpatialJoinCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);
    LOGGER.info("Executing SpatialJoinCommand...");
    try {
      cmd.computeResults(params);
      VoidResponseProtos resp = VoidResponseProtos.newBuilder().build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
    }
  }
}
