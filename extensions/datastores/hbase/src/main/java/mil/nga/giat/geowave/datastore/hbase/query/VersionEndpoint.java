/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos.VersionRequest;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos.VersionResponse;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos.VersionService;

public class VersionEndpoint extends
		VersionService implements
		Coprocessor,
		CoprocessorService
{
	@Override
	public void start(
			final CoprocessorEnvironment env )
			throws IOException {
		// nothing to do when coprocessor is starting up
	}

	@Override
	public void stop(
			final CoprocessorEnvironment env )
			throws IOException {
		// nothing to do when coprocessor is shutting down
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void version(
			final RpcController controller,
			final VersionRequest request,
			final RpcCallback<VersionResponse> done ) {
		done.run(VersionResponse.newBuilder().addAllVersionInfo(
				VersionUtils.getVersionInfo()).build());
	}
}
