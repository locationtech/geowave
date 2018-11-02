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
package org.locationtech.geowave.test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.commons.exec.CommandLine;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.codehaus.mojo.cassandra.AbstractCassandraMojo;
import org.codehaus.mojo.cassandra.StartCassandraClusterMojo;
import org.codehaus.mojo.cassandra.StartCassandraMojo;
import org.codehaus.mojo.cassandra.StopCassandraClusterMojo;
import org.codehaus.mojo.cassandra.StopCassandraMojo;
import org.codehaus.plexus.util.FileUtils;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.util.ClasspathUtils;
import org.locationtech.geowave.datastore.cassandra.CassandraStoreFactoryFamily;
import org.locationtech.geowave.datastore.cassandra.config.CassandraOptions;
import org.locationtech.geowave.datastore.cassandra.config.CassandraRequiredOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStoreTestEnvironment extends
		StoreTestEnvironment
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CassandraStoreTestEnvironment.class);

	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new CassandraStoreFactoryFamily()
			.getDataStoreFactory();
	private static CassandraStoreTestEnvironment singletonInstance = null;
	protected static final File TEMP_DIR = new File(
			System.getProperty("user.dir") + File.separator + "target",
			"cassandra_temp");
	protected static final String NODE_DIRECTORY_PREFIX = "cassandra";

	// for testing purposes, easily toggle between running the cassandra server
	// as multi-nodes or as standalone
	private static final boolean CLUSTERED_MODE = false;

	private static class StartGeoWaveCluster extends
			StartCassandraClusterMojo
	{
		private StartGeoWaveCluster() {
			super();
			startWaitSeconds = 180;
			rpcAddress = "127.0.0.1";
			rpcPort = 9160;
			jmxPort = 7199;
			startNativeTransport = true;
			nativeTransportPort = 9042;
			listenAddress = "127.0.0.1";
			storagePort = 7000;
			stopPort = 8081;
			stopKey = "cassandra-maven-plugin";
			maxMemory = 512;
			cassandraDir = new File(
					TEMP_DIR,
					NODE_DIRECTORY_PREFIX);
			logLevel = "ERROR";
			project = new MavenProject();
			project.setFile(cassandraDir);
			Field f;
			try {
				f = StartCassandraClusterMojo.class.getDeclaredField("clusterSize");
				f.setAccessible(true);
				f.set(
						this,
						4);
				f = AbstractCassandraMojo.class.getDeclaredField("pluginArtifact");

				f.setAccessible(true);
				final DefaultArtifact a = new DefaultArtifact(
						"group",
						"artifact",
						VersionRange.createFromVersionSpec("version"),
						null,
						"type",
						null,
						new DefaultArtifactHandler());
				a.setFile(cassandraDir);
				f.set(
						this,
						a);

				f = AbstractCassandraMojo.class.getDeclaredField("pluginDependencies");
				f.setAccessible(true);
				f.set(
						this,
						new ArrayList<>());
			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException
					| InvalidVersionSpecificationException e) {
				LOGGER.error(
						"Unable to initialize start cassandra cluster",
						e);
			}
		}

		@Override
		protected CommandLine newServiceCommandLine(
				final File cassandraDir,
				final String listenAddress,
				final String rpcAddress,
				final BigInteger initialToken,
				final String[] seeds,
				final boolean jmxRemoteEnabled,
				final int jmxPort )
				throws IOException {
			return super.newServiceCommandLine(
					cassandraDir,
					listenAddress,
					rpcAddress,
					BigInteger.valueOf(initialToken.longValue()),
					seeds,
					false,
					jmxPort);
		}

		@Override
		protected void createCassandraJar(
				final File jarFile,
				final String mainClass,
				final File cassandraDir )
				throws IOException {
			ClasspathUtils.setupPathingJarClassPath(
					jarFile,
					mainClass,
					this.getClass(),
					new File(
							cassandraDir,
							"conf").toURI().toURL());
		}

		public void start() {
			try {
				super.execute();
			}
			catch (MojoExecutionException | MojoFailureException e) {
				LOGGER.error(
						"Unable to start cassandra cluster",
						e);
			}
		}

	}

	private static class StopGeoWaveCluster extends
			StopCassandraClusterMojo
	{
		private StopGeoWaveCluster() {
			super();
			rpcPort = 9160;
			stopPort = 8081;
			stopKey = "cassandra-maven-plugin";
			Field f;
			try {
				f = StopCassandraClusterMojo.class.getDeclaredField("clusterSize");
				f.setAccessible(true);
				f.set(
						this,
						4);
				f = StopCassandraClusterMojo.class.getDeclaredField("rpcAddress");

				f.setAccessible(true);
				f.set(
						this,
						"127.0.0.1");
			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				LOGGER.error(
						"Unable to initialize stop cassandra cluster",
						e);
			}
		}

		public void stop() {
			try {
				super.execute();
			}
			catch (MojoExecutionException | MojoFailureException e) {
				LOGGER.error(
						"Unable to stop cassandra cluster",
						e);
			}
		}
	}

	private static class StartGeoWaveStandalone extends
			StartCassandraMojo
	{
		private StartGeoWaveStandalone() {
			super();
			startWaitSeconds = 180;
			rpcAddress = "127.0.0.1";
			rpcPort = 9160;
			jmxPort = 7199;
			startNativeTransport = true;
			nativeTransportPort = 9042;
			listenAddress = "127.0.0.1";
			storagePort = 7000;
			stopPort = 8081;
			stopKey = "cassandra-maven-plugin";
			maxMemory = 512;
			cassandraDir = TEMP_DIR;
			logLevel = "ERROR";
			project = new MavenProject();
			project.setFile(cassandraDir);
			Field f;
			try {
				f = AbstractCassandraMojo.class.getDeclaredField("pluginArtifact");

				f.setAccessible(true);
				final DefaultArtifact a = new DefaultArtifact(
						"group",
						"artifact",
						VersionRange.createFromVersionSpec("version"),
						null,
						"type",
						null,
						new DefaultArtifactHandler());
				a.setFile(cassandraDir);
				f.set(
						this,
						a);

				f = AbstractCassandraMojo.class.getDeclaredField("pluginDependencies");
				f.setAccessible(true);
				f.set(
						this,
						new ArrayList<>());
			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException
					| InvalidVersionSpecificationException e) {
				LOGGER.error(
						"Unable to initialize start cassandra cluster",
						e);
			}
		}

		@Override
		protected void createCassandraJar(
				final File jarFile,
				final String mainClass,
				final File cassandraDir )
				throws IOException {
			ClasspathUtils.setupPathingJarClassPath(
					jarFile,
					mainClass,
					this.getClass(),
					new File(
							cassandraDir,
							"conf").toURI().toURL());
		}

		public void start() {
			try {
				super.execute();
			}
			catch (MojoExecutionException | MojoFailureException e) {
				LOGGER.error(
						"Unable to start cassandra cluster",
						e);
			}
		}

	}

	private static class StopGeoWaveStandalone extends
			StopCassandraMojo
	{
		public StopGeoWaveStandalone() {
			super();
			rpcPort = 9160;
			stopPort = 8081;
			stopKey = "cassandra-maven-plugin";
			Field f;
			try {
				f = StopCassandraMojo.class.getDeclaredField("rpcAddress");

				f.setAccessible(true);
				f.set(
						this,
						"127.0.0.1");
			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				LOGGER.error(
						"Unable to initialize stop cassandra cluster",
						e);
			}
		}

		public void stop() {
			try {
				super.execute();
			}
			catch (MojoExecutionException | MojoFailureException e) {
				LOGGER.error(
						"Unable to stop cassandra cluster",
						e);
			}
		}
	}

	public static synchronized CassandraStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new CassandraStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private boolean running = false;

	private CassandraStoreTestEnvironment() {}

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {
		final CassandraRequiredOptions cassandraOpts = (CassandraRequiredOptions) options;
		cassandraOpts.setContactPoint("127.0.0.1");
		((CassandraOptions) cassandraOpts.getStoreOptions()).setBatchWriteSize(5);
	}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	public void setup() {
		if (!running) {
			if (TEMP_DIR.exists()) {
				cleanTempDir();
			}
			if (!TEMP_DIR.mkdirs()) {
				LOGGER.warn("Unable to create temporary cassandra directory");
			}
			if (CLUSTERED_MODE) {
				new StartGeoWaveCluster().start();
			}
			else {
				new StartGeoWaveStandalone().start();
			}
			running = true;
		}
	}

	@Override
	public void tearDown() {
		if (running) {
			if (CLUSTERED_MODE) {
				new StopGeoWaveCluster().stop();
			}
			else {
				new StopGeoWaveStandalone().stop();
			}
			running = false;
		}
		try {
			// it seems sometimes one of the nodes processes is still holding
			// onto a file, so wait a short time to be able to reliably clean up
			Thread.sleep(1500);
		}
		catch (final InterruptedException e) {
			LOGGER.warn(
					"Unable to sleep waiting to delete directory",
					e);
		}
		cleanTempDir();
	}

	private static void cleanTempDir() {
		try {
			FileUtils.deleteDirectory(TEMP_DIR);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to delete temp cassandra directory",
					e);
		}
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.CASSANDRA;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}
}
