/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.cli;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.locationtech.geowave.core.store.util.ClasspathUtils;

public final class HBaseMiniClusterClassLoader extends URLClassLoader {
  /**
   * If the resource being loaded matches any of these patterns, we will first attempt to load the
   * resource with the parent ClassLoader. Only if the resource is not found by the parent do we
   * attempt to load it from the coprocessor jar.
   */
  private static final Pattern[] RESOURCE_LOAD_PARENT_FIRST_PATTERNS =
      new Pattern[] {Pattern.compile("^[^-]+-default\\.xml$")};
  /**
   * If the class being loaded starts with any of these strings, we will skip trying to load it from
   * the coprocessor jar and instead delegate directly to the parent ClassLoader.
   */
  // private static final String[] CLASS_PREFIX_EXEMPTIONS =
  // new String[] {
  // // Java standard library:
  // "com.sun.",
  // "sun.",
  // "java.",
  // "jdk.",
  // "javax.",
  // "org.ietf",
  // "org.omg",
  // "org.w3c",
  // "org.xml",
  // "sunw.",
  // // logging
  // "org.apache.commons.logging",
  // "org.apache.logging.log4j",
  // "com.hadoop"};

  /**
   * If the class being loaded starts with any of these strings, we will skip trying to load it from
   * the coprocessor jar and instead delegate directly to the parent ClassLoader.
   */
  private static final String[] CLASS_PREFIX_EXEMPTIONS =
      new String[] {
          // Java standard library:
          "com.sun.",
          "sun.",
          "java.",
          "jdk.",
          "javax.",
          "org.ietf",
          "org.omg",
          "org.w3c",
          "org.xml",
          "sunw.",
          // logging
          "org.apache.commons.logging",
          "org.apache.logging.log4j",
          "com.hadoop",
          // Hadoop/HBase/ZK:
          "org.apache.hadoop.security",
          "org.apache.hadoop.conf",
          "org.apache.hadoop.fs",
          "org.apache.hadoop.util",
          "org.apache.hadoop.io"};
  private static final String[] EXCLUDED_JAR_PREFIXES =
      new String[] {
          "guava",
          "hbase-shaded-client",
          "hbase-shaded-protobuf",
          "protobuf-java",
          "hbase-protocol"};
  private static ClassLoader hbaseMiniClusterCl;

  /**
   * Parent class loader.
   */
  protected final ClassLoader parent;
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(HBaseMiniClusterClassLoader.class);

  public static synchronized ClassLoader getInstance(
      final ClassLoader parentCl,
      final String serversideLib) {
    if (hbaseMiniClusterCl == null) {
      hbaseMiniClusterCl =
          java.security.AccessController.doPrivileged(
              new java.security.PrivilegedAction<ClassLoader>() {
                @Override
                public ClassLoader run() {
                  return new HBaseMiniClusterClassLoader(parentCl, serversideLib);
                }
              });
    }
    return hbaseMiniClusterCl;
  }

  /** Creates a JarClassLoader that loads classes from the given paths. */
  public HBaseMiniClusterClassLoader(final ClassLoader parent, final String serversideLib) {
    super(new URL[] {}, parent);
    this.parent = parent;
    // search for JAR files in the given directory
    final FileFilter jarFilter = new FileFilter() {
      @Override
      public boolean accept(final File pathname) {
        return pathname.getName().endsWith(".jar");
      }
    };

    // create URL for each JAR file found
    final File[] jarFiles = new File(serversideLib).listFiles(jarFilter);

    if (null != jarFiles) {

      for (int i = 0; i < jarFiles.length; i++) {
        try {
          addURL(jarFiles[i].toURI().toURL());
        } catch (final MalformedURLException e) {
          throw new RuntimeException("Could not get URL for JAR file: " + jarFiles[i], e);
        }
      }
    }
    try {
      final String jarPath =
          ClasspathUtils.setupPathingJarClassPath(
              new File(serversideLib),
              HBaseMiniClusterClassLoader.class,
              Arrays.asList(EXCLUDED_JAR_PREFIXES));
      addURL(new File(jarPath).toURI().toURL());
    } catch (final IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return loadClass(name, null);
  }

  public Class<?> loadClass(String name, String[] includedClassPrefixes)
      throws ClassNotFoundException {
    // Delegate to the parent immediately if this class is exempt
    if (isClassExempt(name, includedClassPrefixes)) {
      return parent.loadClass(name);
    }

    synchronized (getClassLoadingLock(name)) {
      // Check whether the class has already been loaded:
      Class<?> clasz = findLoadedClass(name);
      if (clasz != null) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Class " + name + " already loaded");
        }
      } else {
        try {
          // Try to find this class using the URLs passed to this ClassLoader
          clasz = findClass(name);
        } catch (ClassNotFoundException e) {
          // Class not found using this ClassLoader, so delegate to parent
          try {
            clasz = parent.loadClass(name);
          } catch (ClassNotFoundException e2) {
            // Class not found in this ClassLoader or in the parent ClassLoader
            // Log some debug output before re-throwing ClassNotFoundException
            throw e2;
          }
        }
      }
      return clasz;
    }
  }

  @Override
  public URL getResource(String name) {
    URL resource = null;
    boolean parentLoaded = false;

    // Delegate to the parent first if necessary
    if (loadResourceUsingParentFirst(name)) {
      resource = super.getResource(name);
      parentLoaded = true;
    }

    if (resource == null) {
      synchronized (getClassLoadingLock(name)) {
        // Try to find the resource in this jar
        resource = findResource(name);
        if ((resource == null) && !parentLoaded) {
          // Not found in this jar and we haven't attempted to load
          // the resource in the parent yet; fall back to the parent
          resource = super.getResource(name);
        }
      }
    }
    return resource;
  }

  /**
   * Determines whether the given class should be exempt from being loaded by this ClassLoader.
   * 
   * @param name the name of the class to test.
   * @return true if the class should *not* be loaded by this ClassLoader; false otherwise.
   */
  protected boolean isClassExempt(String name, String[] includedClassPrefixes) {
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

  /**
   * Determines whether we should attempt to load the given resource using the parent first before
   * attempting to load the resource using this ClassLoader.
   * 
   * @param name the name of the resource to test.
   * @return true if we should attempt to load the resource using the parent first; false if we
   *         should attempt to load the resource using this ClassLoader first.
   */
  protected boolean loadResourceUsingParentFirst(String name) {
    for (Pattern resourcePattern : RESOURCE_LOAD_PARENT_FIRST_PATTERNS) {
      if (resourcePattern.matcher(name).matches()) {
        return true;
      }
    }
    return false;
  }
}
