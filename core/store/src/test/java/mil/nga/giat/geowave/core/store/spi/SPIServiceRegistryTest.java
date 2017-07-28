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
package mil.nga.giat.geowave.core.store.spi;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.core.index.SPIServiceRegistry;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.commons.vfs2.provider.jar.JarFileProvider;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.junit.Test;

public class SPIServiceRegistryTest
{

	@Test
	public void test()
			// HPFortify FP: unit test not subject to security vulnerability
			throws FileSystemException,
			MalformedURLException {
		DefaultFileSystemManager fsManager = new DefaultFileSystemManager();
		fsManager.setCacheStrategy(CacheStrategy.MANUAL);
		fsManager.setFilesCache(new SoftRefFilesCache());
		fsManager.addProvider(
				"file",
				new DefaultLocalFileProvider());
		fsManager.addProvider(
				"jar",
				new JarFileProvider());
		fsManager.setBaseFile(new File(
				"."));

		// fsManager.addProvider("jar", new JarFileProvider());
		fsManager.init();
		FileObject jarFile = fsManager.resolveFile("jar:src/test/test.jar");

		final VFSClassLoader cl = new VFSClassLoader(
				jarFile,
				fsManager);

		final FileObject[] fileObjs = cl.getFileObjects();
		final URL[] fileUrls = new URL[fileObjs.length];
		for (int i = 0; i < fileObjs.length; i++) {
			fileUrls[i] = new URL(
					fileObjs[i].toString());
		}
		SPIServiceRegistry registry = new SPIServiceRegistry(
				FieldSerializationProviderSpi.class);
		registry.registerLocalClassLoader(java.security.AccessController
				.doPrivileged(new java.security.PrivilegedAction<URLClassLoader>() {
					public URLClassLoader run() {
						final URLClassLoader ucl = new URLClassLoader(
								fileUrls,
								cl);
						return ucl;
					}
				}));

		// Proves that the VFS Classloader SPI loading bug exists
		Iterator<FieldSerializationProviderSpi> it1 = ServiceLoader.load(
				FieldSerializationProviderSpi.class).iterator();
		boolean found = false;
		while (it1.hasNext()) {
			found |= it1.next().getClass().equals(
					BooleanSerializationProvider.class);
		}
		assertFalse(found);

		Iterator<FieldSerializationProviderSpi> it = registry.load(FieldSerializationProviderSpi.class);
		found = false;
		try {
			while (it.hasNext()) {
				found |= it.next().getClass().equals(
						BooleanSerializationProvider.class);
			}
		}
		catch (Throwable ex) {

			assertTrue(ex instanceof NoClassDefFoundError && ex.getLocalizedMessage().contains(
					"FieldSerializationProviderSpi"));
			return;
		}
		assertTrue(
				"The class not found exception is expected since the JAR file was not registered",
				false);
	}
}
