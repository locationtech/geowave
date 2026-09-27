/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.eclipse.jetty.ee.webapp.WebAppClassLoader;
import org.eclipse.jetty.ee11.webapp.WebAppContext;

/**
 * GeoServer's web application, run from an unpacked WAR on Jetty's ee11 (Servlet 6.1) environment,
 * with GeoWave added from this JVM's class path.
 *
 * <p> GeoWave's class path entries follow the WAR's own WEB-INF/lib, where an installed plugin
 * would be, so GeoServer runs on the libraries it ships with and GeoWave's copies only supply what
 * the WAR does not have. A jar the WAR has under the same file name is left out altogether, which
 * also keeps GeoServer's own Spring configuration from being loaded twice.
 */
public class GeoServerWebAppContext extends WebAppContext {
  public static final String CONTEXT_PATH = "/geoserver";
  // lets the styles form post large SLDs
  private static final int MAX_FORM_CONTENT_SIZE = 1024 * 1024 * 2;
  // Jetty and the servlet API are the container's. GeoServer uses neither Jakarta REST nor Spark,
  // and Scala stays with Spark. HBase's static initializers need HBase and its dependencies in one
  // class loader, the parent, while GeoWave's HBase data store has to load with GeoTools, in the
  // web application.
  private static final Pattern LEFT_IN_PARENT =
      Pattern.compile(
          "(jetty|jakarta\\.servlet|javax\\.servlet|servlet-api|jakarta\\.ws\\.rs|jersey|hk2|spark|scala|hbase|hadoop|protobuf).*");

  private final List<Path> geowaveClasspath;

  public GeoServerWebAppContext(final Path warDirectory) {
    this(warDirectory, System.getProperty("java.class.path"));
  }

  GeoServerWebAppContext(final Path warDirectory, final String classpath) {
    setContextPath(CONTEXT_PATH);
    setWar(warDirectory.toString());
    setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);
    geowaveClasspath = geowaveClasspath(warDirectory.resolve("WEB-INF").resolve("lib"), classpath);
  }

  List<Path> getGeoWaveClasspath() {
    return Collections.unmodifiableList(geowaveClasspath);
  }

  private static List<Path> geowaveClasspath(final Path webInfLib, final String classpath) {
    final Set<String> warLibraries = new HashSet<>();
    final File[] libraries = webInfLib.toFile().listFiles();
    if (libraries != null) {
      for (final File library : libraries) {
        warLibraries.add(library.getName());
      }
    }
    final List<Path> entries = new ArrayList<>();
    for (final String entry : classpath.split(File.pathSeparator)) {
      if (entry.isEmpty()) {
        continue;
      }
      final Path path = Paths.get(entry).toAbsolutePath();
      if (Files.isDirectory(path)) {
        entries.add(path);
      } else if (Files.isRegularFile(path)) {
        final String name = path.toFile().getName();
        if (!warLibraries.contains(name)
            && (name.startsWith("geowave") || !LEFT_IN_PARENT.matcher(name).matches())) {
          entries.add(path);
        }
      }
    }
    return entries;
  }

  @Override
  public boolean configure() throws Exception {
    // the configurations have now put WEB-INF/classes and WEB-INF/lib on the class loader
    final boolean configured = super.configure();
    final WebAppClassLoader classLoader = (WebAppClassLoader) getClassLoader();
    for (final Path entry : geowaveClasspath) {
      classLoader.addClassPath(getResourceFactory().newResource(entry));
    }
    return configured;
  }
}
