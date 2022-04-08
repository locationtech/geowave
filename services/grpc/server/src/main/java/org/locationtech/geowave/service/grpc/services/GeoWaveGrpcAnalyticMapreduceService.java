/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.grpc.services;

import java.io.File;
import java.util.Map;
import org.locationtech.geowave.analytic.mapreduce.operations.DBScanCommand;
import org.locationtech.geowave.analytic.mapreduce.operations.KdeCommand;
import org.locationtech.geowave.analytic.mapreduce.operations.NearestNeighborCommand;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc;
import org.locationtech.geowave.service.grpc.protobuf.DBScanCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.KdeCommandParametersProtos;
import org.locationtech.geowave.service.grpc.protobuf.NearestNeighborCommandParametersProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

public class GeoWaveGrpcAnalyticMapreduceService extends
    AnalyticMapreduceGrpc.AnalyticMapreduceImplBase implements
    GeoWaveGrpcServiceSpi {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcAnalyticMapreduceService.class.getName());

  @Override
  public BindableService getBindableService() {
    return this;
  }

  @Override
  public void kdeCommand(
      final KdeCommandParametersProtos request,
      final StreamObserver<VoidResponseProtos> responseObserver) {
    final KdeCommand cmd = new KdeCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);
    try {
      cmd.computeResults(params);
    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
    LOGGER.info("Executing KdeCommand...");
    try {

      final VoidResponseProtos resp = VoidResponseProtos.newBuilder().build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void dBScanCommand(
      final DBScanCommandParametersProtos request,
      final StreamObserver<VoidResponseProtos> responseObserver) {
    final DBScanCommand cmd = new DBScanCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);
    LOGGER.info("Executing DBScanCommand...");
    try {
      cmd.computeResults(params);
      final VoidResponseProtos resp = VoidResponseProtos.newBuilder().build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void nearestNeighborCommand(
      final NearestNeighborCommandParametersProtos request,
      final StreamObserver<VoidResponseProtos> responseObserver) {
    final NearestNeighborCommand cmd = new NearestNeighborCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);
    LOGGER.info("Executing NearestNeighborCommand...");
    try {
      cmd.computeResults(params);
      final VoidResponseProtos resp = VoidResponseProtos.newBuilder().build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }
}
