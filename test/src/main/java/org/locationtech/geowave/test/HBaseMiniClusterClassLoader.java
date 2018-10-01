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
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.locationtech.geowave.core.store.util.ClasspathUtils;

public class HBaseMiniClusterClassLoader extends
		URLClassLoader
{

	/**
	 * If the class being loaded starts with any of these strings, we will skip
	 * trying to load it from the coprocessor jar and instead delegate directly
	 * to the parent ClassLoader.
	 */
	private static final String[] CLASS_PREFIX_EXEMPTIONS = new String[] {
		// Java standard library:
		"com.sun.",
		"sun.",
		"java.",
		"javax.",
		"org.ietf",
		"org.omg",
		"org.w3c",
		"org.xml",
		"sunw.",
		// logging
		"org.apache.commons.logging",
		"org.apache.log4j",
		"com.hadoop",
		// Hadoop/HBase/ZK:
		"org.apache.hadoop.security",
		"org.apache.hadoop.conf",
		"org.apache.hadoop.fs",
		"org.apache.hadoop.util",
		"org.apache.hadoop.io"
	};

	private static ClassLoader hbaseMiniClusterCl;

	public static synchronized ClassLoader getInstance(
			final ClassLoader parentCl ) {
		if (hbaseMiniClusterCl == null) {
			hbaseMiniClusterCl = java.security.AccessController
					.doPrivileged(new java.security.PrivilegedAction<ClassLoader>() {
						@Override
						public ClassLoader run() {
							return new HBaseMiniClusterClassLoader(
									parentCl);
						}
					});
		}
		return hbaseMiniClusterCl;
	}

	/**
	 * Creates a JarClassLoader that loads classes from the given paths.
	 */
	public HBaseMiniClusterClassLoader(
			ClassLoader parent ) {
		super(
				new URL[] {},
				parent);
		// search for JAR files in the given directory
		FileFilter jarFilter = new FileFilter() {
			public boolean accept(
					File pathname ) {
				return pathname.getName().endsWith(
						".jar");
			}
		};

		// create URL for each JAR file found
		File[] jarFiles = new File(
				"target/hbase/lib").listFiles(jarFilter);

		if (null != jarFiles) {

			for (int i = 0; i < jarFiles.length; i++) {
				try {
					addURL(jarFiles[i].toURI().toURL());
				}
				catch (MalformedURLException e) {
					throw new RuntimeException(
							"Could not get URL for JAR file: " + jarFiles[i],
							e);
				}
			}

		}
		try {
			final String jarPath = ClasspathUtils.setupPathingJarClassPath(
					new File(
							"target/hbase/lib"),
					HBaseMiniClusterClassLoader.class);
			addURL(new File(
					jarPath).toURI().toURL());
		}
		catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public Class<?> loadClass(
			String name )
			throws ClassNotFoundException {
		if (isClassExempt(
				name,
				null)) {
			return getParent().loadClass(
					name);
		}
		synchronized (getClassLoadingLock(name)) {
			// Check whether the class has already been loaded:
			Class<?> clasz = findLoadedClass(name);
			if (clasz != null) {
				return clasz;
			}
			try {
				// Try to find this class using the URLs passed to this
				// ClassLoader
				clasz = findClass(name);
			}
			catch (ClassNotFoundException e) {
				// Class not found using this ClassLoader, so delegate to parent
				try {
					clasz = getParent().loadClass(
							name);
				}
				catch (ClassNotFoundException e2) {
					// Class not found in this ClassLoader or in the parent
					// ClassLoader
					// Log some debug output before re-throwing
					// ClassNotFoundException
					throw e2;
				}
			}
			return clasz;
		}
	}

	/**
	 * Determines whether the given class should be exempt from being loaded by
	 * this ClassLoader.
	 * 
	 * @param name
	 *            the name of the class to test.
	 * @return true if the class should *not* be loaded by this ClassLoader;
	 *         false otherwise.
	 */
	protected boolean isClassExempt(
			String name,
			String[] includedClassPrefixes ) {
		if (includedClassPrefixes != null) {
			for (String clsName : includedClassPrefixes) {
				if (name.startsWith(clsName)) {
					return false;
				}
			}
		}
		for (String exemptPrefix : CLASS_PREFIX_EXEMPTIONS) {
			if (name.startsWith(exemptPrefix)) {
				return true;
			}
		}
		return false;
	}
}
