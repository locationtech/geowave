/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.osm;

import java.io.File;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.cli.osm.operations.IngestOSMToGeoWaveCommand;
import org.locationtech.geowave.cli.osm.operations.StageOSMToHDFSCommand;
import org.locationtech.geowave.core.store.cli.store.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.test.AccumuloStoreTestEnvironment;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.mapreduce.MapReduceTestEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.MAP_REDUCE})
public class MapReduceIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceIT.class);

  protected static final String TEST_RESOURCE_DIR =
      new File("./src/test/resources/osm/").getAbsolutePath().toString();
  protected static final String TEST_DATA_ZIP_RESOURCE_PATH =
      TEST_RESOURCE_DIR + "/" + "andorra-latest.zip";
  protected static final String TEST_DATA_BASE_DIR =
      new File(TestUtils.TEST_CASE_BASE, "osm").getAbsoluteFile().toString();

  @GeoWaveTestStore({GeoWaveStoreType.ACCUMULO})
  protected DataStorePluginOptions dataStoreOptions;

  @BeforeClass
  public static void setupTestData() throws ZipException {
    final ZipFile data = new ZipFile(new File(TEST_DATA_ZIP_RESOURCE_PATH));
    data.extractAll(TEST_DATA_BASE_DIR);
  }

  @Test
  public void testIngestOSMPBF() throws Exception {
    TestUtils.deleteAll(dataStoreOptions);
    // NOTE: This will probably fail unless you bump up the memory for the
    // tablet
    // servers, for whatever reason, using the
    // miniAccumuloConfig.setMemory() function.
    final MapReduceTestEnvironment mrEnv = MapReduceTestEnvironment.getInstance();

    // TODO: for now this only works with accumulo, generalize the data
    // store usage
    final AccumuloStoreTestEnvironment accumuloEnv = AccumuloStoreTestEnvironment.getInstance();

    final String hdfsPath = mrEnv.getHdfsBaseDirectory() + "/osm_stage/";

    final StageOSMToHDFSCommand stage = new StageOSMToHDFSCommand();
    stage.setParameters(TEST_DATA_BASE_DIR, hdfsPath);
    stage.execute(mrEnv.getOperationParams());

    final Connector conn =
        new ZooKeeperInstance(
            accumuloEnv.getAccumuloInstance(),
            accumuloEnv.getZookeeper()).getConnector(
                accumuloEnv.getAccumuloUser(),
                new PasswordToken(accumuloEnv.getAccumuloPassword()));
    final Authorizations auth = new Authorizations(new String[] {"public"});
    conn.securityOperations().changeUserAuthorizations(accumuloEnv.getAccumuloUser(), auth);
    final IngestOSMToGeoWaveCommand ingest = new IngestOSMToGeoWaveCommand();
    ingest.setParameters(hdfsPath, "test-store");

    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test-store");
    addStore.setPluginOptions(dataStoreOptions);
    addStore.execute(mrEnv.getOperationParams());

    ingest.getIngestOptions().setJobName("ConversionTest");

    // Execute for node's ways, and relations.
    ingest.getIngestOptions().setMapperType("NODE");
    ingest.execute(mrEnv.getOperationParams());
    System.out.println("finished accumulo ingest Node");

    ingest.getIngestOptions().setMapperType("WAY");
    ingest.execute(mrEnv.getOperationParams());
    System.out.println("finished accumulo ingest Way");

    ingest.getIngestOptions().setMapperType("RELATION");
    ingest.execute(mrEnv.getOperationParams());
    System.out.println("finished accumulo ingest Relation");
  }
}
