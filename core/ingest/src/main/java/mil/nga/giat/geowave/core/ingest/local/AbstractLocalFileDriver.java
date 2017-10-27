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
package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.lang.reflect.Field;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.upplication.s3fs.S3Path;

import mil.nga.giat.geowave.core.ingest.DataAdapterProvider;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

/**
 * This class can be sub-classed to handle recursing over a local directory
 * structure and passing along the plugin specific handling of any supported
 * file for a discovered plugin.
 * 
 * @param <P>
 *            The type of the plugin this driver supports.
 * @param <R>
 *            The type for intermediate data that can be used throughout the
 *            life of the process and is passed along for each call to process a
 *            file.
 */
abstract public class AbstractLocalFileDriver<P extends LocalPluginBase, R>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractLocalFileDriver.class);
	protected LocalInputCommandLineOptions localInput;

	public AbstractLocalFileDriver(
			LocalInputCommandLineOptions input ) {
		localInput = input;
	}

	protected boolean checkIndexesAgainstProvider(
			String providerName,
			DataAdapterProvider<?> adapterProvider,
			List<IndexPluginOptions> indexOptions ) {
		boolean valid = true;
		for (IndexPluginOptions option : indexOptions) {
			if (!IngestUtils.isCompatible(
					adapterProvider,
					option)) {
				// HP Fortify "Log Forging" false positive
				// What Fortify considers "user input" comes only
				// from users with OS-level access anyway
				LOGGER.warn("Local file ingest plugin for ingest type '" + providerName
						+ "' does not support dimensionality '" + option.getType() + "'");
				valid = false;
			}
		}
		return valid;
	}

	public void setURLStreamHandleFactory()
			throws NoSuchFieldException,
			SecurityException,
			IllegalArgumentException,
			IllegalAccessException {

		Field factoryField = URL.class.getDeclaredField("factory");
		factoryField.setAccessible(true);

		URLStreamHandlerFactory urlStreamHandlerFactory = (URLStreamHandlerFactory) factoryField.get(null);

		if (urlStreamHandlerFactory == null) {
			URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory());

		}
		else {
			Field lockField = URL.class.getDeclaredField("streamHandlerLock");
			lockField.setAccessible(true);
			synchronized (lockField.get(null)) {

				factoryField.set(
						null,
						null);
				URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory(
						urlStreamHandlerFactory));
			}
		}

	}

	protected void processInput(
			final String inputPath,
			final Map<String, P> localPlugins,
			final R runData )
			throws IOException {
		if (inputPath == null) {
			LOGGER.error("Unable to ingest data, base directory or file input not specified");
			return;
		}

		Path path = null;
		// If input path is S3
		if (inputPath.startsWith("s3://")) {
			try {
				setURLStreamHandleFactory();
			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e1) {
				LOGGER.error("Error in setting up S3URLStreamHandle Factory");
				return;
			}
			try {
				FileSystem fs = FileSystems.newFileSystem(
						new URI(
								"s3://s3.amazonaws.com/"),
						new HashMap<String, Object>(),
						Thread.currentThread().getContextClassLoader());
				String s3InputPath = inputPath.replaceFirst(
						"s3://",
						"/");
				path = (S3Path) fs.getPath(s3InputPath);
				if (!path.isAbsolute()) {
					LOGGER.error("Input file " + inputPath + " does not exist");
					return;
				}
			}
			catch (URISyntaxException e) {
				LOGGER.error("Unable to ingest data, Inavlid S3 URI");
				return;
			}
		}
		else {
			final File f = new File(
					inputPath);
			if (!f.exists()) {
				LOGGER.error("Input file '" + f.getAbsolutePath() + "' does not exist");
				throw new IllegalArgumentException(
						inputPath + " does not exist");
			}
			path = Paths.get(inputPath);
		}

		for (final LocalPluginBase localPlugin : localPlugins.values()) {
			localPlugin.init(path.toUri().toURL());
		}

		Files.walkFileTree(
				// Paths.get(inputPath),
				path,
				new LocalPluginFileVisitor<P, R>(
						localPlugins,
						this,
						runData,
						localInput.getExtensions()));
	}

	abstract protected void processFile(
			final URL file,
			String typeName,
			P plugin,
			R runData )
			throws IOException;
}
