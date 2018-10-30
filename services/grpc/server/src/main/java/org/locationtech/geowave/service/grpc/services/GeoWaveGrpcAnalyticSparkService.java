/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.service.grpc.services;

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
import org.locationtech.geowave.service.grpc.protobuf.KmeansSparkCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.SparkSqlCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.SpatialJoinCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class GeoWaveGrpcAnalyticSparkService extends
		AnalyticSparkGrpc.AnalyticSparkImplBase implements
		GeoWaveGrpcServiceSpi
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcAnalyticSparkService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void kmeansSparkCommand(
			KmeansSparkCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		KmeansSparkCommand cmd = new KmeansSparkCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);
		LOGGER.info("Executing KmeansSparkCommand...");
		try {
			cmd.computeResults(params);
			VoidResponse resp = VoidResponse.newBuilder().build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void sparkSqlCommand(
			SparkSqlCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		SparkSqlCommand cmd = new SparkSqlCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);
		LOGGER.info("Executing SparkSqlCommand...");
		try {
			cmd.computeResults(params);
			VoidResponse resp = VoidResponse.newBuilder().build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

	@Override
	public void spatialJoinCommand(
			SpatialJoinCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		SpatialJoinCommand cmd = new SpatialJoinCommand();
		Map<FieldDescriptor, Object> m = request.getAllFields();
		GeoWaveGrpcServiceCommandUtil.setGrpcToCommandFields(
				m,
				cmd);

		final File configFile = GeoWaveGrpcServiceOptions.geowaveConfigFile;
		final OperationParams params = new ManualOperationParams();
		params.getContext().put(
				ConfigOptions.PROPERTIES_FILE_CONTEXT,
				configFile);

		cmd.prepare(params);
		LOGGER.info("Executing SpatialJoinCommand...");
		try {
			cmd.computeResults(params);
			VoidResponse resp = VoidResponse.newBuilder().build();
			responseObserver.onNext(resp);
			responseObserver.onCompleted();

		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
	}

}
