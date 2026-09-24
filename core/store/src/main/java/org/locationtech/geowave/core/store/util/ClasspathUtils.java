/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
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

public class ClasspathUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClasspathUtils.class);
  private static List<ClassLoaderTransformerSpi> transformerList = null;

  public static String setupPathingJarClassPath(
      final File dir,
      final Class context,
      final URL... additionalClasspathUrls) throws IOException {
    return setupPathingJarClassPath(
        new File(dir.getParentFile().getAbsolutePath() + File.separator + "pathing", "pathing.jar"),
        null,
        context,
        additionalClasspathUrls);
  }

  public static String setupPathingJarClassPath(
      final File jarFile,
      final String mainClass,
      final Class context,
      final URL... additionalClasspathUrls) throws IOException {

    final File jarDir = jarFile.getParentFile();
    final String classpath = getClasspath(context, additionalClasspathUrls);

    if (!jarDir.exists()) {
      try {
        jarDir.mkdirs();
      } catch (final Exception e) {
        LOGGER.error("Failed to create pathing jar directory: " + e);
        return null;
      }
    }

    if (jarFile.exists()) {
      try {
        jarFile.delete();
      } catch (final Exception e) {
        LOGGER.error("Failed to delete old pathing jar: " + e);
        return null;
      }
    }

    // build jar
    final Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, classpath);
    if (mainClass != null) {
      manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, mainClass);
    }
    // HP Fortify "Improper Resource Shutdown or Release" false positive
    // target is inside try-as-resource clause (and is auto-closeable) and
    // the FileOutputStream
    // is closed implicitly by target.close()
    try (final JarOutputStream target =
        new JarOutputStream(new FileOutputStream(jarFile), manifest)) {

      target.close();
    }

    return jarFile.getAbsolutePath();
  }

  /**
   * Builds the class path to write into a pathing jar's manifest.
   *
   * <p> This used to walk the classloader chain and demand that every loader be a
   * {@link URLClassLoader} or a {@link VFSClassLoader}, throwing otherwise. From JDK 9 the
   * application and platform loaders are {@code jdk.internal.loader.ClassLoaders$AppClassLoader}
   * and {@code $PlatformClassLoader}, neither of which is a URLClassLoader, so it threw "Unknown
   * classloader type" on every JDK above 8 with no flag able to prevent it. Every integration lane
   * reaches this, through GeoWaveMultiProcessIngestIT as well as the HBase and Accumulo mini
   * cluster factories.
   *
   * <p> The system class path is now read from {@code java.class.path}, which is what the
   * application loader is built from anyway and which works on every release. Loaders that carry
   * entries the system class path does not know about -- VFSClassLoader, and any other
   * URLClassLoader in the chain -- are still walked and contribute their URLs. A loader of any
   * other kind is skipped rather than fatal: the JDK's own built-in loaders are exactly that case,
   * and their contents are already covered by java.class.path.
   */
  private static String getClasspath(final Class context, final URL... additionalUrls)
      throws IOException {

    try {
      final StringBuilder classpathBuilder = new StringBuilder();
      for (final URL u : additionalUrls) {
        append(classpathBuilder, u);
      }

      for (final String entry : System.getProperty("java.class.path", "").split(
          File.pathSeparator)) {
        if (!entry.isEmpty()) {
          append(classpathBuilder, new File(entry).toURI().toURL());
        }
      }

      final ArrayList<ClassLoader> classloaders = new ArrayList<>();
      ClassLoader cl = context.getClassLoader();
      while (cl != null) {
        classloaders.add(cl);
        cl = cl.getParent();
      }
      Collections.reverse(classloaders);

      for (final ClassLoader classLoader : classloaders) {
        if (classLoader instanceof VFSClassLoader) {
          for (final FileObject f : ((VFSClassLoader) classLoader).getFileObjects()) {
            append(classpathBuilder, f.getURL());
          }
        } else if (classLoader instanceof URLClassLoader) {
          for (final URL u : ((URLClassLoader) classLoader).getURLs()) {
            append(classpathBuilder, u);
          }
        }
      }

      if (classpathBuilder.length() == 0) {
        return "";
      }
      classpathBuilder.deleteCharAt(0);
      return classpathBuilder.toString();

    } catch (final URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private static void append(final StringBuilder classpathBuilder, final URL url)
      throws URISyntaxException {

    final File file = new File(url.toURI());

    // do not include dirs containing hadoop or accumulo site files
    if (!containsSiteFile(file)) {
      final int index = file.getAbsolutePath().indexOf(":\\");
      String windowsFriendlyPath;
      if (index > 0) {
        windowsFriendlyPath =
            "file:/"
                + file.getAbsolutePath().substring(0, index)
                + ":/"
                + file.getAbsolutePath().substring(index + 2);
      } else {
        windowsFriendlyPath = file.getAbsolutePath();
      }

      classpathBuilder.append(" ").append(windowsFriendlyPath.replace("\\", "/"));
      if (file.isDirectory()) {
        classpathBuilder.append("/");
      }
    }
  }

  private static boolean containsSiteFile(final File f) {
    if (f.isDirectory()) {
      final File[] sitefile = f.listFiles(new FileFilter() {
        @Override
        public boolean accept(final File pathname) {
          return pathname.getName().endsWith("site.xml");
        }
      });

      return (sitefile != null) && (sitefile.length > 0);
    }
    return false;
  }

  public static synchronized ClassLoader transformClassLoader(final ClassLoader classLoader) {
    if (transformerList == null) {
      final Iterator<ClassLoaderTransformerSpi> transformers =
          SPIServiceRegistry.load(ClassLoaderTransformerSpi.class);
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
