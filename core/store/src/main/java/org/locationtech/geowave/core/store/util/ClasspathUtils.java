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
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Collection;

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

  public static String setupPathingJarClassPath(
      final File dir,
      final Class context,
      final Collection<String> excludedJarPrefixes,
      final URL... additionalClasspathUrls) throws IOException {
    return setupPathingJarClassPath(
        new File(dir.getParentFile().getAbsolutePath() + File.separator + "pathing", "pathing.jar"),
        null,
        context,
        excludedJarPrefixes,
        additionalClasspathUrls);
  }

  public static String setupPathingJarClassPath(
      final File jarFile,
      final String mainClass,
      final Class context,
      final Collection<String> excludedJarPrefixes,
      final URL... additionalClasspathUrls) throws IOException {

    final File jarDir = jarFile.getParentFile();
    final String classpath = getClasspath(context, excludedJarPrefixes, additionalClasspathUrls);

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

  private static String getClasspath(final Class context, final URL... additionalUrls)
      throws IOException {
    return getClasspath(context, (Collection<String>) null, additionalUrls);
  }

  private static String getClasspath(
      final Class context,
      final Collection<String> excludedJarPrefixes,
      final URL... additionalUrls) throws IOException {

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
        append(classpathBuilder, u, Collections.emptySet());
      }

      boolean foundAnyUrls = false;

      // Process each classloader
      for (int i = 0; i < classloaders.size(); i++) {
        final ClassLoader classLoader = classloaders.get(i);

        if (classLoader instanceof URLClassLoader) {
          // Java 8 and custom URLClassLoaders (including HBaseMiniClusterClassLoader)
          for (final URL u : ((URLClassLoader) classLoader).getURLs()) {
            append(classpathBuilder, u, excludedJarPrefixes);
            foundAnyUrls = true;
          }
        } else if (classLoader instanceof VFSClassLoader) {
          final VFSClassLoader vcl = (VFSClassLoader) classLoader;
          for (final FileObject f : vcl.getFileObjects()) {
            append(classpathBuilder, f.getURL(), excludedJarPrefixes);
            foundAnyUrls = true;
          }
        } else {
          // Java 9+ classloaders (AppClassLoader, PlatformClassLoader)
          // Try to get URLs via reflection
          URL[] urls = null;

          // Try the BuiltinClassLoader.ucp field (Java 9-16)
          try {
            final Field ucpField = classLoader.getClass().getDeclaredField("ucp");
            ucpField.setAccessible(true);
            final Object ucp = ucpField.get(classLoader);
            if (ucp != null) {
              final Method getURLsMethod = ucp.getClass().getMethod("getURLs");
              urls = (URL[]) getURLsMethod.invoke(ucp);
            }
          } catch (Exception e) {
            // Field/method not available, try alternative approach
          }

          // Alternative: try getURLs() method directly (some custom loaders)
          if (urls == null) {
            try {
              final Method getUrlsMethod = classLoader.getClass().getMethod("getURLs");
              urls = (URL[]) getUrlsMethod.invoke(classLoader);
            } catch (Exception e) {
              // Method not available
            }
          }

          if (urls != null) {
            for (final URL u : urls) {
              append(classpathBuilder, u, excludedJarPrefixes);
              foundAnyUrls = true;
            }
          }
        }
      }

      // If we didn't find any URLs from classloaders, fall back to system classpath
      if (!foundAnyUrls || classpathBuilder.length() == 0) {
        final String sysCp = System.getProperty("java.class.path");
        if (sysCp != null && !sysCp.isEmpty()) {
          final String[] parts = sysCp.split(java.io.File.pathSeparator);
          for (final String part : parts) {
            if (part == null || part.isEmpty())
              continue;
            try {
              append(classpathBuilder, new java.io.File(part).toURI().toURL(), excludedJarPrefixes);
            } catch (java.net.MalformedURLException e) {
              // skip invalid entry
            }
          }
        }
      }

      // Remove leading space if present
      if (classpathBuilder.length() > 0 && classpathBuilder.charAt(0) == ' ') {
        classpathBuilder.deleteCharAt(0);
      }

      return classpathBuilder.toString();

    } catch (final URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private static void append(
      final StringBuilder classpathBuilder,
      final URL url,
      final Collection<String> excludedJarPrefixes) throws URISyntaxException {

    final File file = new File(url.toURI());

    // Skip excluded JARs by filename prefix
    if (isExcludedJar(file, excludedJarPrefixes)) {
      return;
    }

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

  private static boolean isExcludedJar(
      final File file,
      final Collection<String> excludedJarPrefixes) {
    if (excludedJarPrefixes == null || excludedJarPrefixes.isEmpty()) {
      return false;
    }
    if (!file.isFile()) {
      return false;
    }
    final String name = file.getName();
    if (!name.endsWith(".jar")) {
      return false;
    }
    for (final String prefix : excludedJarPrefixes) {
      if (prefix != null && !prefix.isEmpty() && name.startsWith(prefix)) {
        return true;
      }
    }
    return false;
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
      final SPIServiceRegistry registry = new SPIServiceRegistry();
      final Iterator<ClassLoaderTransformerSpi> transformers =
          registry.load(ClassLoaderTransformerSpi.class);
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
