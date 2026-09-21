/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.util;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * The pathing jar is built for every integration lane, through GeoWaveMultiProcessIngestIT and the
 * HBase and Accumulo mini cluster factories. Building it used to demand that every loader in the
 * chain be a URLClassLoader or a VFSClassLoader, which stopped being true in JDK 9 when the
 * application and platform loaders became jdk.internal.loader.ClassLoaders$AppClassLoader and
 * $PlatformClassLoader.
 */
public class ClasspathUtilsTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void buildsAPathingJarOnWhateverLoaderTheRuntimeUses() throws Exception {
    final File dir = folder.newFolder("target");
    final String jarPath = ClasspathUtils.setupPathingJarClassPath(dir, ClasspathUtilsTest.class);

    final File jar = new File(jarPath);
    assertTrue("no pathing jar was written to " + jarPath, jar.isFile());

    try (JarFile jarFile = new JarFile(jar)) {
      final Manifest manifest = jarFile.getManifest();
      final String classPath = manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
      assertTrue("the manifest carries no Class-Path", (classPath != null) && !classPath.isEmpty());
      // Whatever else is on it, the entry holding this very class has to be there, or the
      // subprocess the jar exists for cannot start.
      assertTrue(
          "the manifest does not mention core/store's own classes: " + classPath,
          classPath.contains("core/store") || classPath.contains("geowave-core-store"));
    }
  }
}
