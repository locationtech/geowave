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
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.ingest.operations.KafkaToGeoWaveCommand;
import org.locationtech.geowave.core.ingest.operations.ListIngestPluginsCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToGeoWaveCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToHdfsCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToKafkaCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToMapReduceToGeoWaveCommand;
import org.locationtech.geowave.core.ingest.operations.MapReduceToGeoWaveCommand;
import org.locationtech.geowave.core.ingest.operations.SparkToGeoWaveCommand;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.CoreIngestGrpc.CoreIngestImplBase;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

public class GeoWaveGrpcCoreIngestService extends CoreIngestImplBase implements
    GeoWaveGrpcServiceSpi {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcCoreIngestService.class.getName());

  @Override
  public BindableService getBindableService() {
    return this;
  }

  @Override
  public void localToHdfsCommand(
      final org.locationtech.geowave.service.grpc.protobuf.LocalToHdfsCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos> responseObserver) {

    final LocalToHdfsCommand cmd = new LocalToHdfsCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing LocalToHdfsCommand...");
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
  public void sparkToGeoWaveCommand(
      final org.locationtech.geowave.service.grpc.protobuf.SparkToGeoWaveCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos> responseObserver) {
    final SparkToGeoWaveCommand cmd = new SparkToGeoWaveCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    if (!cmd.prepare(params)) {
      LOGGER.error("Failed to prepare parameters for SparkToGeowaveCommand");
    }

    LOGGER.info("Executing SparkToGeowaveCommand...");
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
  public void kafkaToGeoWaveCommand(
      final org.locationtech.geowave.service.grpc.protobuf.KafkaToGeoWaveCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos> responseObserver) {

    final KafkaToGeoWaveCommand cmd = new KafkaToGeoWaveCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing KafkaToGeowaveCommand...");
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
  public void listIngestPluginsCommand(
      final org.locationtech.geowave.service.grpc.protobuf.ListIngestPluginsCommandParametersProtos request,
      final io.grpc.stub.StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos> responseObserver) {

    final ListIngestPluginsCommand cmd = new ListIngestPluginsCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing ListPluginsCommand...");
    try {
      final String result = cmd.computeResults(params);
      final StringResponseProtos resp =
          StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void localToKafkaCommand(
      final org.locationtech.geowave.service.grpc.protobuf.LocalToKafkaCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos> responseObserver) {

    final LocalToKafkaCommand cmd = new LocalToKafkaCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing LocalToKafkaCommand...");
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
  public void localToMapReduceToGeoWaveCommand(
      final org.locationtech.geowave.service.grpc.protobuf.LocalToMapReduceToGeoWaveCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos> responseObserver) {
    final LocalToMapReduceToGeoWaveCommand cmd = new LocalToMapReduceToGeoWaveCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing LocalToMapReduceToGeowaveCommand...");
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
  public void localToGeoWaveCommand(
      final org.locationtech.geowave.service.grpc.protobuf.LocalToGeoWaveCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos> responseObserver) {

    final LocalToGeoWaveCommand cmd = new LocalToGeoWaveCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing LocalToGeowaveCommand...");
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
  public void mapReduceToGeoWaveCommand(
      final org.locationtech.geowave.service.grpc.protobuf.MapReduceToGeoWaveCommandParametersProtos request,
      final StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos> responseObserver) {

    final MapReduceToGeoWaveCommand cmd = new MapReduceToGeoWaveCommand();
    final Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing MapReduceToGeowaveCommand...");
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
