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
package mil.nga.giat.geowave.datastore.accumulo.minicluster;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.util.ClasspathUtils;

public class MiniAccumuloClusterFactory
{

	private static final Logger LOGGER = LoggerFactory.getLogger(MiniAccumuloClusterFactory.class);

	protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";

	protected static boolean isYarn() {
		return VersionUtil.compareVersions(
				VersionInfo.getVersion(),
				"2.2.0") >= 0;
	}

	public static MiniAccumuloClusterImpl newAccumuloCluster(
			final MiniAccumuloConfigImpl config,
			final Class context )
			throws IOException {

		final String jarPath = ClasspathUtils.setupPathingJarClassPath(
				config.getDir(),
				context);

		if (jarPath == null) {
			// Jar was not successfully created
			return null;
		}

		config.setClasspathItems(jarPath);

		final MiniAccumuloClusterImpl retVal = new GeoWaveMiniAccumuloClusterImpl(
				config);
		if (SystemUtils.IS_OS_WINDOWS && isYarn()) {
			// this must happen after instantiating Mini
			// Accumulo Cluster because it ensures the accumulo
			// directory is empty or it will fail, but must
			// happen before the cluster is started because yarn
			// expects winutils.exe to exist within a bin
			// directory in the mini accumulo cluster directory
			// (mini accumulo cluster will always set this
			// directory as hadoop_home)
			LOGGER.info("Running YARN on windows requires a local installation of Hadoop");
			LOGGER.info("'HADOOP_HOME' must be set and 'PATH' must contain %HADOOP_HOME%/bin");

			final Map<String, String> env = System.getenv();
			// HP Fortify "Path Manipulation" false positive
			// What Fortify considers "user input" comes only
			// from users with OS-level access anyway
			String hadoopHome = System.getProperty("hadoop.home.dir");
			if (hadoopHome == null) {
				hadoopHome = env.get("HADOOP_HOME");
			}
			boolean success = false;
			if (hadoopHome != null) {
				// HP Fortify "Path Traversal" false positive
				// What Fortify considers "user input" comes only
				// from users with OS-level access anyway
				final File hadoopDir = new File(
						hadoopHome);
				if (hadoopDir.exists()) {
					final File binDir = new File(
							config.getDir(),
							"bin");
					if (binDir.mkdir()) {
						FileUtils.copyFile(
								new File(
										hadoopDir + File.separator + "bin",
										HADOOP_WINDOWS_UTIL),
								new File(
										binDir,
										HADOOP_WINDOWS_UTIL));
						success = true;
					}
				}
			}
			if (!success) {
				LOGGER
						.error("'HADOOP_HOME' environment variable is not set or <HADOOP_HOME>/bin/winutils.exe does not exist");

				// return mini accumulo cluster anyways
				return retVal;
			}
		}
		return retVal;
	}
}
