/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.ClasspathUtils;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import com.google.common.collect.Iterators;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveMultiProcessIngestIT extends AbstractGeoWaveBasicVectorIT {
  private static int NUM_PROCESSES = 4;
  protected static final File TEMP_DIR = new File("./target/multiprocess_temp");
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.REDIS,
      // these data stores don't seem to properly pass the environment using Hadoop config and could
      // be investigated further
      // GeoWaveStoreType.DYNAMODB,
      // GeoWaveStoreType.KUDU,
      // GeoWaveStoreType.BIGTABLE
      })
  protected DataStorePluginOptions dataStorePluginOptions;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }

  @Test
  public void testMultiProcessIngest() throws Exception {
    for (int j = 0; j < 10; j++) {
      final Class<?> clazz = GeoWaveMultiProcessIngestIT.class;
      final String javaHome = System.getProperty("java.home");
      final String javaBin = javaHome + File.separator + "bin" + File.separator + "java";

      final String className = clazz.getName();
      final String jarFile = ClasspathUtils.setupPathingJarClassPath(TEMP_DIR, clazz);

      final Index idx1 = SimpleIngest.createSpatialIndex();
      final Index idx2 = SimpleIngest.createSpatialTemporalIndex();

      final DataStore store = dataStorePluginOptions.createDataStore();
      store.addIndex(idx1);
      store.addIndex(idx2);
      final StringBuilder indexNames = new StringBuilder();
      indexNames.append(idx1.getName()).append(",").append(idx2.getName());
      final Configuration conf = new Configuration();
      conf.set(AbstractMapReduceIngest.INDEX_NAMES_KEY, indexNames.toString());
      for (final MetadataType type : MetadataType.values()) {
        // stats and index metadata writers are created elsewhere
        if (!MetadataType.INDEX.equals(type) && !MetadataType.STATISTIC_VALUES.equals(type)) {
          dataStorePluginOptions.createDataStoreOperations().createMetadataWriter(type).close();
        }
      }
      GeoWaveOutputFormat.addIndex(conf, idx1);
      GeoWaveOutputFormat.addIndex(conf, idx2);
      GeoWaveOutputFormat.setStoreOptions(conf, dataStorePluginOptions);
      Assert.assertTrue(TEMP_DIR.exists() || TEMP_DIR.mkdirs());
      final File configFile = new File(TEMP_DIR, "hadoop-job.conf");
      Assert.assertTrue(!configFile.exists() || configFile.delete());
      Assert.assertTrue(configFile.createNewFile());
      try (DataOutputStream dataOut = new DataOutputStream(new FileOutputStream(configFile))) {
        conf.write(dataOut);
      }
      final List<ProcessBuilder> bldrs = new ArrayList<>();
      for (int i = 0; i < NUM_PROCESSES; i++) {
        final ArrayList<String> argList = new ArrayList<>();
        argList.addAll(
            Arrays.asList(javaBin, "-cp", jarFile, className, new Integer(i * 10000).toString()));
        final ProcessBuilder builder = new ProcessBuilder(argList);
        builder.directory(TEMP_DIR);
        builder.inheritIO();
        bldrs.add(builder);
      }
      final List<Process> processes = bldrs.stream().map(b -> {
        try {
          return b.start();
        } catch (final IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        return null;
      }).collect(Collectors.toList());
      Assert.assertFalse(processes.stream().anyMatch(Objects::isNull));
      processes.forEach(p -> {
        try {
          p.waitFor();
        } catch (final InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      });
      try (CloseableIterator<Object> it = store.query(QueryBuilder.newBuilder().build())) {
        Assert.assertEquals(2701 * NUM_PROCESSES, Iterators.size(it));
      }
      try (CloseableIterator<SimpleFeature> it =
          store.query(VectorQueryBuilder.newBuilder().indexName(idx1.getName()).build())) {
        Assert.assertEquals(2701 * NUM_PROCESSES, Iterators.size(it));
      }
      try (CloseableIterator<SimpleFeature> it =
          store.query(VectorQueryBuilder.newBuilder().indexName(idx2.getName()).build())) {
        Assert.assertEquals(2701 * NUM_PROCESSES, Iterators.size(it));
      }
      try (CloseableIterator<SimpleFeature> it =
          store.query(
              VectorQueryBuilder.newBuilder().constraints(
                  VectorQueryBuilder.newBuilder().constraintsFactory().spatialTemporalConstraints().spatialConstraints(
                      GeometryUtils.GEOMETRY_FACTORY.toGeometry(
                          new Envelope(-172, 172, -82, 82))).build()).build())) {
        Assert.assertEquals(2277 * NUM_PROCESSES, Iterators.size(it));
      }
      final long epochTime = 1609459200000L;

      final long startTime = epochTime + TimeUnit.DAYS.toMillis(15);
      final long endTime = epochTime + TimeUnit.DAYS.toMillis(345);
      try (CloseableIterator<SimpleFeature> it =
          store.query(
              VectorQueryBuilder.newBuilder().constraints(
                  VectorQueryBuilder.newBuilder().constraintsFactory().spatialTemporalConstraints().spatialConstraints(
                      GeometryUtils.GEOMETRY_FACTORY.toGeometry(
                          new Envelope(-172, 172, -82, 82))).addTimeRange(
                              new Date(startTime),
                              new Date(endTime)).build()).build())) {
        Assert.assertEquals(2178 * NUM_PROCESSES, Iterators.size(it));
      }

      TestUtils.deleteAll(getDataStorePluginOptions());
    }
  }

  public static void main(final String[] args)
      throws FileNotFoundException, IOException, InterruptedException {
    final int featureId = Integer.parseInt(args[0]);
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final GeotoolsFeatureDataAdapter<SimpleFeature> fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        SimpleIngest.getGriddedFeatures(new SimpleFeatureBuilder(sft), featureId);
    final Configuration conf = new Configuration();
    final File configFile = new File("hadoop-job.conf");

    try (DataInputStream dataIn = new DataInputStream(new FileInputStream(configFile))) {
      conf.readFields(dataIn);
    }

    final JobContextImpl context = new JobContextImpl(conf, null);
    final DataStorePluginOptions dataStorePluginOptions =
        GeoWaveOutputFormat.getStoreOptions(context);
    final GeoWaveOutputFormat.GeoWaveRecordWriter writer =
        new GeoWaveOutputFormat.GeoWaveRecordWriter(
            dataStorePluginOptions.createDataStore(),
            GeoWaveOutputFormat.getJobContextIndexStore(context),
            (TransientAdapterStore) GeoWaveOutputFormat.getJobContextAdapterStore(context));

    final String[] indexNames = AbstractMapReduceIngest.getIndexNames(context.getConfiguration());
    for (final SimpleFeature f : features) {
      writer.write(new GeoWaveOutputKey<>(fda, indexNames), f);
    }
    writer.close(null);
    System.exit(0);
  }
}
