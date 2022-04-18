/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.locationtech.geowave.core.cli.VersionUtils;
import org.locationtech.geowave.datastore.hbase.query.protobuf.VersionProtosServer.VersionRequest;
import org.locationtech.geowave.datastore.hbase.query.protobuf.VersionProtosServer.VersionResponse;
import org.locationtech.geowave.datastore.hbase.query.protobuf.VersionProtosServer.VersionService;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class VersionEndpoint extends VersionService implements RegionCoprocessor {
  @Override
  public void start(final CoprocessorEnvironment env) throws IOException {
    // nothing to do when coprocessor is starting up
  }

  @Override
  public void stop(final CoprocessorEnvironment env) throws IOException {
    // nothing to do when coprocessor is shutting down
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singletonList(this);
  }

  @Override
  public void version(
      final RpcController controller,
      final VersionRequest request,
      final RpcCallback<VersionResponse> done) {
    done.run(VersionResponse.newBuilder().addAllVersionInfo(VersionUtils.getVersionInfo()).build());
  }
}
