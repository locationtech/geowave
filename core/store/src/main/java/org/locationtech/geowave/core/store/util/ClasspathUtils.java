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
package org.locationtech.geowave.core.store.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.store.spi.ClassLoaderTransformerSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClasspathUtils
{

	private static final Logger LOGGER = LoggerFactory.getLogger(ClasspathUtils.class);
	private static List<ClassLoaderTransformerSpi> transformerList = null;

	public static String setupPathingJarClassPath(
			final File dir,
			final Class context,
			final URL... additionalClasspathUrls )
			throws IOException {
		return setupPathingJarClassPath(
				new File(
						dir.getParentFile().getAbsolutePath() + File.separator + "pathing",
						"pathing.jar"),
				null,
				context,
				additionalClasspathUrls);
	}

	public static String setupPathingJarClassPath(
			final File jarFile,
			final String mainClass,
			final Class context,
			final URL... additionalClasspathUrls )
			throws IOException {

		final File jarDir = jarFile.getParentFile();
		final String classpath = getClasspath(
				context,
				additionalClasspathUrls);

		if (!jarDir.exists()) {
			try {
				jarDir.mkdirs();
			}
			catch (final Exception e) {
				LOGGER.error("Failed to create pathing jar directory: " + e);
				return null;
			}
		}

		if (jarFile.exists()) {
			try {
				jarFile.delete();
			}
			catch (final Exception e) {
				LOGGER.error("Failed to delete old pathing jar: " + e);
				return null;
			}
		}

		// build jar
		final Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(
				Attributes.Name.MANIFEST_VERSION,
				"1.0");
		manifest.getMainAttributes().put(
				Attributes.Name.CLASS_PATH,
				classpath);
		if (mainClass != null) {
			manifest.getMainAttributes().put(
					Attributes.Name.MAIN_CLASS,
					mainClass);
		}
		// HP Fortify "Improper Resource Shutdown or Release" false positive
		// target is inside try-as-resource clause (and is auto-closeable) and
		// the FileOutputStream
		// is closed implicitly by target.close()
		try (final JarOutputStream target = new JarOutputStream(
				new FileOutputStream(
						jarFile),
				manifest)) {

			target.close();
		}

		return jarFile.getAbsolutePath();
	}

	private static String getClasspath(
			final Class context,
			final URL... additionalUrls )
			throws IOException {

		try {
			final ArrayList<ClassLoader> classloaders = new ArrayList<>();

			ClassLoader cl = context.getClassLoader();

			while (cl != null) {
				classloaders.add(cl);
				cl = cl.getParent();
			}

			Collections.reverse(classloaders);

			final StringBuilder classpathBuilder = new StringBuilder();
			for (final URL u : additionalUrls) {
				append(
						classpathBuilder,
						u);
			}

			// assume 0 is the system classloader and skip it
			for (int i = 0; i < classloaders.size(); i++) {
				final ClassLoader classLoader = classloaders.get(i);

				if (classLoader instanceof URLClassLoader) {

					for (final URL u : ((URLClassLoader) classLoader).getURLs()) {
						append(
								classpathBuilder,
								u);
					}

				}
				else if (classLoader instanceof VFSClassLoader) {

					final VFSClassLoader vcl = (VFSClassLoader) classLoader;
					for (final FileObject f : vcl.getFileObjects()) {
						append(
								classpathBuilder,
								f.getURL());
					}
				}
				else {
					throw new IllegalArgumentException(
							"Unknown classloader type : " + classLoader.getClass().getName());
				}
			}

			classpathBuilder.deleteCharAt(0);
			return classpathBuilder.toString();

		}
		catch (final URISyntaxException e) {
			throw new IOException(
					e);
		}
	}

	private static void append(
			final StringBuilder classpathBuilder,
			final URL url )
			throws URISyntaxException {

		final File file = new File(
				url.toURI());

		// do not include dirs containing hadoop or accumulo site files
		if (!containsSiteFile(file)) {
			classpathBuilder.append(
					" ").append(
					file.getAbsolutePath().replace(
							"C:\\",
							"file:/C:/").replace(
							"\\",
							"/"));
			if (file.isDirectory()) {
				classpathBuilder.append("/");
			}
		}
	}

	private static boolean containsSiteFile(
			final File f ) {
		if (f.isDirectory()) {
			final File[] sitefile = f.listFiles(new FileFilter() {
				@Override
				public boolean accept(
						final File pathname ) {
					return pathname.getName().endsWith(
							"site.xml");
				}
			});

			return (sitefile != null) && (sitefile.length > 0);
		}
		return false;
	}

	public static synchronized ClassLoader transformClassLoader(
			final ClassLoader classLoader ) {
		if (transformerList == null) {
			final Iterator<ClassLoaderTransformerSpi> transformers = new SPIServiceRegistry(
					ClassLoaderTransformerSpi.class).load(ClassLoaderTransformerSpi.class);
			transformerList = new ArrayList<>();
			while (transformers.hasNext()) {
				transformerList.add(transformers.next());
			}
		}
		for (final ClassLoaderTransformerSpi t : transformerList) {
			final ClassLoader cl = t.transform(classLoader);
			if (cl != null) {
				return cl;
			}
		}
		return null;

	}
}
