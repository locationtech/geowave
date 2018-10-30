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

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Descriptors.FieldDescriptor;

import org.locationtech.geowave.analytic.mapreduce.operations.DBScanCommand;
import org.locationtech.geowave.analytic.mapreduce.operations.KdeCommand;
import org.locationtech.geowave.analytic.mapreduce.operations.NearestNeighborCommand;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceOptions;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServiceSpi;
import org.locationtech.geowave.service.grpc.protobuf.AnalyticMapreduceGrpc;
import org.locationtech.geowave.service.grpc.protobuf.DBScanCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.KdeCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.NearestNeighborCommandParameters;
import org.locationtech.geowave.service.grpc.protobuf.GeoWaveReturnTypes.VoidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveGrpcAnalyticMapreduceService extends
		AnalyticMapreduceGrpc.AnalyticMapreduceImplBase implements
		GeoWaveGrpcServiceSpi
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcAnalyticMapreduceService.class.getName());

	@Override
	public BindableService getBindableService() {
		return (BindableService) this;
	}

	@Override
	public void kdeCommand(
			KdeCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		KdeCommand cmd = new KdeCommand();
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
		try {
			cmd.computeResults(params);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Exception encountered executing command",
					e);
		}
		LOGGER.info("Executing KdeCommand...");
		try {

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
	public void dBScanCommand(
			DBScanCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		DBScanCommand cmd = new DBScanCommand();
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
		LOGGER.info("Executing DBScanCommand...");
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
	public void nearestNeighborCommand(
			NearestNeighborCommandParameters request,
			StreamObserver<VoidResponse> responseObserver ) {
		NearestNeighborCommand cmd = new NearestNeighborCommand();
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
		LOGGER.info("Executing NearestNeighborCommand...");
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
