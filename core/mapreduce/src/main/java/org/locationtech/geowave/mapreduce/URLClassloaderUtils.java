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
package org.locationtech.geowave.mapreduce;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.util.ClasspathUtils;
import org.locationtech.geowave.mapreduce.hdfs.HdfsUrlStreamHandlerFactory;
import org.locationtech.geowave.mapreduce.s3.S3URLStreamHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLClassloaderUtils
{
	private static final Logger LOGGER = LoggerFactory.getLogger(URLClassloaderUtils.class);
	private static final Object MUTEX = new Object();
	private static Set<ClassLoader> initializedClassLoaders = new HashSet<>();

	public static enum URLTYPE {
		S3,
		HDFS
	}

	private static boolean hasS3Handler = false;
	private static boolean hasHdfsHandler = false;

	public static void setURLStreamHandlerFactory(
			final URLTYPE urlType )
			throws NoSuchFieldException,
			SecurityException,
			IllegalArgumentException,
			IllegalAccessException {
		// One-time init for each type
		if ((urlType == URLTYPE.S3) && hasS3Handler) {
			return;
		}
		else if ((urlType == URLTYPE.HDFS) && hasHdfsHandler) {
			return;
		}

		final Field factoryField = URL.class.getDeclaredField("factory");
		// HP Fortify "Access Control" false positive
		// The need to change the accessibility here is
		// necessary, has been review and judged to be safe
		factoryField.setAccessible(true);

		final URLStreamHandlerFactory urlStreamHandlerFactory = (URLStreamHandlerFactory) factoryField.get(null);

		if (urlStreamHandlerFactory == null) {
			if (urlType == URLTYPE.S3) {
				URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory());
				hasS3Handler = true;
			}
			else { // HDFS
				URL.setURLStreamHandlerFactory(new HdfsUrlStreamHandlerFactory());
				hasHdfsHandler = true;
			}

		}
		else {
			final Field lockField = URL.class.getDeclaredField("streamHandlerLock");
			// HP Fortify "Access Control" false positive
			// The need to change the accessibility here is
			// necessary, has been review and judged to be safe
			lockField.setAccessible(true);
			synchronized (lockField.get(null)) {

				factoryField.set(
						null,
						null);

				if (urlType == URLTYPE.S3) {
					URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory(
							urlStreamHandlerFactory));
					hasS3Handler = true;
				}
				else { // HDFS
					URL.setURLStreamHandlerFactory(new HdfsUrlStreamHandlerFactory(
							urlStreamHandlerFactory));
					hasHdfsHandler = true;
				}
			}
		}
	}

	public static void initClassLoader()
			throws MalformedURLException {
		synchronized (MUTEX) {
			ClassLoader myCl = URLClassloaderUtils.class.getClassLoader();
			if (initializedClassLoaders.contains(myCl)) {
				return;
			}
			final ClassLoader classLoader = ClasspathUtils.transformClassLoader(myCl);
			if (classLoader != null) {
				SPIServiceRegistry.registerClassLoader(classLoader);
			}
			initializedClassLoaders.add(myCl);
		}
	}

	protected static boolean verifyProtocol(
			final String fileStr ) {
		if (fileStr.contains("s3://")) {
			try {
				setURLStreamHandlerFactory(URLTYPE.S3);

				return true;
			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e1) {
				LOGGER.error(
						"Error in setting up S3URLStreamHandler Factory",
						e1);

				return false;
			}
		}
		else if (fileStr.contains("hdfs://")) {
			try {
				setURLStreamHandlerFactory(URLTYPE.HDFS);

				return true;
			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e1) {
				LOGGER.error(
						"Error in setting up HdfsUrlStreamHandler Factory",
						e1);

				return false;
			}
		}

		LOGGER.debug("Assuming good URLStreamHandler for " + fileStr);
		return true;
	}

	public static byte[] toBinary(
			final Persistable persistable ) {
		try {
			initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.warn(
					"Unable to initialize classloader in toBinary",
					e);
		}
		return PersistenceUtils.toBinary(persistable);
	}

	public static Persistable fromBinary(
			final byte[] bytes ) {
		try {
			initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.warn(
					"Unable to initialize classloader in fromBinary",
					e);
		}
		return PersistenceUtils.fromBinary(bytes);
	}

	public static byte[] toBinary(
			final Collection<? extends Persistable> persistables ) {
		try {
			initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.warn(
					"Unable to initialize classloader in toBinary (list)",
					e);
		}
		return PersistenceUtils.toBinary(persistables);
	}

	public static byte[] toClassId(
			final Persistable persistable ) {
		try {
			initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.warn(
					"Unable to initialize classloader in toClassId",
					e);
		}
		return PersistenceUtils.toClassId(persistable);
	}

	public static Persistable fromClassId(
			final byte[] bytes ) {
		try {
			initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.warn(
					"Unable to initialize classloader in fromClassId",
					e);
		}
		return PersistenceUtils.fromClassId(bytes);
	}

	public static byte[] toClassId(
			final String className ) {
		try {
			initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.warn(
					"Unable to initialize classloader in toClassId(className)",
					e);
		}
		return PersistenceUtils.toClassId(className);
	}

	public static List<Persistable> fromBinaryAsList(
			final byte[] bytes ) {
		try {
			initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.warn(
					"Unable to initialize classloader in fromBinaryAsList",
					e);
		}
		return PersistenceUtils.fromBinaryAsList(bytes);

	}
}
