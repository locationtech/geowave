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
package mil.nga.giat.geowave.test.services;

import java.io.File;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestEnvironment;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;

public class ServicesTestEnvironment implements
		TestEnvironment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicesTestEnvironment.class);

	private static ServicesTestEnvironment singletonInstance = null;

	public static synchronized ServicesTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new ServicesTestEnvironment();
		}
		return singletonInstance;
	}

	private static String[] PARENT_CLASSLOADER_LIBRARIES = new String[] {
		"hbase",
		"hadoop",
		"protobuf",
		"guava",
		"restlet"
	};

	protected static final int JETTY_PORT = 9011;
	protected static final String JETTY_BASE_URL = "http://localhost:" + JETTY_PORT;
	protected static final int ACCEPT_QUEUE_SIZE = 100;
	protected static final int MAX_IDLE_TIME = (int) TimeUnit.HOURS.toMillis(1);
	protected static final int SO_LINGER_TIME = -1;
	protected static final int MAX_FORM_CONTENT_SIZE = 1024 * 1024 * 2;
	protected static final String GEOSERVER_USER = "admin";
	protected static final String GEOSERVER_PASS = "geoserver";
	protected static final String TEST_WORKSPACE = "geowave_test";
	protected static final String GEOSERVER_WAR_DIR = "target/geoserver";
	protected static final String GEOSERVER_CONTEXT_PATH = "/geoserver";
	protected static final String GEOSERVER_BASE_URL = JETTY_BASE_URL + GEOSERVER_CONTEXT_PATH;
	protected static final String GEOSERVER_REST_PATH = GEOSERVER_BASE_URL + "/rest";
	protected static final String GEOWAVE_WAR_DIR = "target/restservices";
	protected static final String GEOWAVE_CONTEXT_PATH = "/restservices";
	protected static final String GEOWAVE_BASE_URL = JETTY_BASE_URL + GEOWAVE_CONTEXT_PATH;

	protected static final String GEOWAVE_CONFIG_FILE = GEOWAVE_WAR_DIR + "/config.properties";
	protected static final String GEOWAVE_WORKSPACE_PATH = GEOSERVER_WAR_DIR + "/data/workspaces/" + TEST_WORKSPACE;
	protected static final String TEST_STYLE_NAME_NO_DIFFERENCE = "SubsamplePoints-2px";
	protected static final String TEST_STYLE_NAME_MINOR_SUBSAMPLE = "SubsamplePoints-10px";
	protected static final String TEST_STYLE_NAME_MAJOR_SUBSAMPLE = "SubsamplePoints-100px";
	protected static final String TEST_STYLE_NAME_DISTRIBUTED_RENDER = "DistributedRender";
	protected static final String TEST_STYLE_PATH = "src/test/resources/sld/";
	protected static final String TEST_GEOSERVER_LOGGING_PATH = "src/test/resources/logging.xml";
	protected static final String TEST_LOG_PROPERTIES_PATH = "src/test/resources/log4j-test.properties";
	protected static final String TEST_GEOSERVER_LOG_PROPERTIES_PATH = GEOSERVER_WAR_DIR
			+ "/data/logs/log4j-test.properties";
	protected static final String EXISTING_GEOSERVER_LOGGING_PATH = GEOSERVER_WAR_DIR + "/data/logging.xml";
	protected static final String TEST_SLD_NO_DIFFERENCE_FILE = TEST_STYLE_PATH + TEST_STYLE_NAME_NO_DIFFERENCE
			+ ".sld";
	protected static final String TEST_SLD_MINOR_SUBSAMPLE_FILE = TEST_STYLE_PATH + TEST_STYLE_NAME_MINOR_SUBSAMPLE
			+ ".sld";
	protected static final String TEST_SLD_MAJOR_SUBSAMPLE_FILE = TEST_STYLE_PATH + TEST_STYLE_NAME_MAJOR_SUBSAMPLE
			+ ".sld";
	protected static final String TEST_SLD_DISTRIBUTED_RENDER_FILE = TEST_STYLE_PATH
			+ TEST_STYLE_NAME_DISTRIBUTED_RENDER + ".sld";

	private Server jettyServer;

	@SuppressFBWarnings(value = {
		"SWL_SLEEP_WITH_LOCK_HELD"
	}, justification = "Jetty must be started before releasing the lock")
	@Override
	public void setup()
			throws Exception {
		synchronized (GeoWaveITRunner.MUTEX) {
			// Setup activities delegated to private function
			// to satisfy HP Fortify
			doSetup();
		}
	}

	private void doSetup() {
		if (jettyServer == null) {
			try {
				// Prevent "Unauthorized class found" error
				System.setProperty(
						"GEOSERVER_XSTREAM_WHITELIST",
						"org.geoserver.wfs.**;org.geoserver.wms.**");

				// delete old workspace configuration if it's still there
				jettyServer = new Server();

				final ServerConnector conn = new ServerConnector(
						jettyServer);
				conn.setPort(JETTY_PORT);
				conn.setAcceptQueueSize(ACCEPT_QUEUE_SIZE);
				conn.setIdleTimeout(MAX_IDLE_TIME);
				conn.setSoLingerTime(SO_LINGER_TIME);
				jettyServer.setConnectors(new Connector[] {
					conn
				});
				FileUtils.copyFile(
						new File(
								TEST_GEOSERVER_LOGGING_PATH),
						new File(
								EXISTING_GEOSERVER_LOGGING_PATH));
				FileUtils.copyFile(
						new File(
								TEST_LOG_PROPERTIES_PATH),
						new File(
								TEST_GEOSERVER_LOG_PROPERTIES_PATH));
				final WebAppContext gsWebapp = new WebAppContext();
				gsWebapp.setContextPath(GEOSERVER_CONTEXT_PATH);
				gsWebapp.setResourceBase(GEOSERVER_WAR_DIR);

				final WebAppClassLoader classLoader = AccessController
						.doPrivileged(new PrivilegedAction<WebAppClassLoader>() {
							@Override
							public WebAppClassLoader run() {
								try {
									return new WebAppClassLoader(
											gsWebapp);
								}
								catch (final IOException e) {
									LOGGER.error(
											"Unable to create new classloader",
											e);
									return null;
								}
							}
						});
				if (classLoader == null) {
					throw new IOException(
							"Unable to create classloader");
				}
				final String classpath = System.getProperty(
						"java.class.path").replace(
						":",
						";");
				final String[] individualEntries = classpath.split(";");
				final StringBuffer str = new StringBuffer();
				for (final String e : individualEntries) {
					// HBase has certain static initializers that use reflection
					// to get annotated values

					// because Class instances are not equal if they are loaded
					// by different class loaders this HBase initialization
					// fails

					// furthermore HBase's runtime dependencies need to
					// be loaded by the same classloader, the webapp's parent
					// class loader

					// but geowave hbase datastore implementation must be loaded
					// by the same classloader as geotools or the SPI loader
					// won't work

					boolean addLibraryToWebappContext = true;
					if (!e.contains("geowave")) {
						for (final String parentLoaderLibrary : PARENT_CLASSLOADER_LIBRARIES) {
							if (e.contains(parentLoaderLibrary)) {
								addLibraryToWebappContext = false;
								break;
							}
						}
					}
					if (addLibraryToWebappContext) {
						str.append(
								e).append(
								";");
					}
				}
				classLoader.addClassPath(str.toString());
				gsWebapp.setClassLoader(classLoader);
				// this has to be false for geoserver to load the correct guava
				// classes (until hadoop updates guava support to a later
				// version, slated for hadoop 3.x)
				gsWebapp.setParentLoaderPriority(false);

				final WebAppContext restWebapp = new WebAppContext();
				restWebapp.setContextPath(GEOWAVE_CONTEXT_PATH);
				restWebapp.setWar(GEOWAVE_WAR_DIR);
				restWebapp.setInitParameter(
						"config_file",
						GEOWAVE_CONFIG_FILE);
				jettyServer.setHandler(new ContextHandlerCollection(
						gsWebapp,
						restWebapp));
				// // this allows to send large SLD's from the styles form
				gsWebapp.getServletContext().getContextHandler().setMaxFormContentSize(
						MAX_FORM_CONTENT_SIZE);

				jettyServer.start();
				while (!jettyServer.isRunning() && !jettyServer.isStarted()) {
					Thread.sleep(1000);
				}

			}
			catch (final RuntimeException e) {
				throw e;
			}
			catch (final Exception e) {
				LOGGER.error(
						"Could not start the Jetty server: " + e.getMessage(),
						e);

				if (jettyServer.isRunning()) {
					try {
						jettyServer.stop();
					}
					catch (final Exception e1) {
						LOGGER.error(
								"Unable to stop the Jetty server",
								e1);
					}
				}
			}
		}
	}

	@Override
	public void tearDown()
			throws Exception {
		synchronized (GeoWaveITRunner.MUTEX) {
			if (!GeoWaveITRunner.DEFER_CLEANUP.get()) {
				if (jettyServer != null) {
					try {
						jettyServer.stop();
						jettyServer = null;
					}
					catch (final Exception e) {
						LOGGER.error(
								"Unable to stop the Jetty server",
								e);
					}
				}
			}
		}
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {
			MapReduceTestEnvironment.getInstance()
		};
	}
}
