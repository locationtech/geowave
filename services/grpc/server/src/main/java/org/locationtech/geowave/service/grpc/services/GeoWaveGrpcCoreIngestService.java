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
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.ingest.operations.KafkaToGeowaveCommand;
import org.locationtech.geowave.core.ingest.operations.ListPluginsCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToGeowaveCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToHdfsCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToKafkaCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToMapReduceToGeowaveCommand;
import org.locationtech.geowave.core.ingest.operations.MapReduceToGeowaveCommand;
import org.locationtech.geowave.core.ingest.operations.SparkToGeowaveCommand;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.CoreIngestGrpc.CoreIngestImplBase;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveGrpcCoreIngestService extends CoreIngestImplBase
    implements GeoWaveGrpcServiceSpi {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveGrpcCoreIngestService.class.getName());

  @Override
  public BindableService getBindableService() {
    return (BindableService) this;
  }

  @Override
  public void localToHdfsCommand(
      org.locationtech.geowave.service.grpc.protobuf.LocalToHdfsCommandParametersProtos request,
      StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos>
          responseObserver) {

    LocalToHdfsCommand cmd = new LocalToHdfsCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
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
    }
  }

  @Override
  public void sparkToGeowaveCommand(
      org.locationtech.geowave.service.grpc.protobuf.SparkToGeowaveCommandParametersProtos request,
      StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos>
          responseObserver) {
    SparkToGeowaveCommand cmd = new SparkToGeowaveCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
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
    }
  }

  @Override
  public void kafkaToGeowaveCommand(
      org.locationtech.geowave.service.grpc.protobuf.KafkaToGeowaveCommandParametersProtos request,
      StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos>
          responseObserver) {

    KafkaToGeowaveCommand cmd = new KafkaToGeowaveCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
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
    }
  }

  @Override
  public void listPluginsCommand(
      org.locationtech.geowave.service.grpc.protobuf.ListPluginsCommandParametersProtos request,
      io.grpc.stub.StreamObserver<
              org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.StringResponseProtos>
          responseObserver) {

    ListPluginsCommand cmd = new ListPluginsCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
    GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(m, cmd);

    final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
    final OperationParams params = new ManualOperationParams();
    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    cmd.prepare(params);

    LOGGER.info("Executing ListPluginsCommand...");
    try {
      String result = cmd.computeResults(params);
      final StringResponseProtos resp = StringResponseProtos.newBuilder().setResponseValue(result).build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();

    } catch (final Exception e) {
      LOGGER.error("Exception encountered executing command", e);
    }
  }

  @Override
  public void localToKafkaCommand(
      org.locationtech.geowave.service.grpc.protobuf.LocalToKafkaCommandParametersProtos request,
      StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos>
          responseObserver) {

    LocalToKafkaCommand cmd = new LocalToKafkaCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
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
    }
  }

  @Override
  public void localToMapReduceToGeowaveCommand(
      org.locationtech.geowave.service.grpc.protobuf.LocalToMapReduceToGeowaveCommandParametersProtos
          request,
      StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos>
          responseObserver) {
    LocalToMapReduceToGeowaveCommand cmd = new LocalToMapReduceToGeowaveCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
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
    }
  }

  @Override
  public void localToGeowaveCommand(
      org.locationtech.geowave.service.grpc.protobuf.LocalToGeowaveCommandParametersProtos request,
      StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos>
          responseObserver) {

    LocalToGeowaveCommand cmd = new LocalToGeowaveCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
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
    }
  }

  @Override
  public void mapReduceToGeowaveCommand(
      org.locationtech.geowave.service.grpc.protobuf.MapReduceToGeowaveCommandParametersProtos request,
      StreamObserver<org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypesProtos.VoidResponseProtos>
          responseObserver) {

    MapReduceToGeowaveCommand cmd = new MapReduceToGeowaveCommand();
    Map<FieldDescriptor, Object> m = request.getAllFields();
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
    }
  }
}
