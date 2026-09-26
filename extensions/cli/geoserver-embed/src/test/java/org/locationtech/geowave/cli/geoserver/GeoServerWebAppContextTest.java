/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.eclipse.jetty.server.Server;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GeoServerWebAppContextTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private Path war;
  private Path classpathDir;

  @Before
  public void unpackedWar() throws IOException {
    war = tempFolder.newFolder("geoserver").toPath();
    final Path lib = Files.createDirectories(war.resolve("WEB-INF").resolve("lib"));
    Files.write(
        war.resolve("WEB-INF").resolve("web.xml"),
        "<web-app/>".getBytes(StandardCharsets.UTF_8));
    jar(lib.resolve("guava-33.5.0-jre.jar"), "which.txt", "war");
    jar(lib.resolve("gt-main-35.1.jar"), "gt-main.txt", "war");
    classpathDir = tempFolder.newFolder("lib").toPath();
  }

  private static Path jar(final Path file, final String entry, final String content)
      throws IOException {
    try (OutputStream out = Files.newOutputStream(file);
        JarOutputStream jar = new JarOutputStream(out)) {
      jar.putNextEntry(new JarEntry(entry));
      jar.write(content.getBytes(StandardCharsets.UTF_8));
      jar.closeEntry();
    }
    return file;
  }

  private Path classpathJar(final String name) throws IOException {
    return jar(classpathDir.resolve(name), name + ".txt", name);
  }

  private static String classpath(final Path... entries) {
    return String.join(
        File.pathSeparator,
        Arrays.stream(entries).map(Path::toString).toArray(String[]::new));
  }

  private static String read(final URL url) throws IOException {
    try (InputStream in = url.openStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void addsWhatTheWarDoesNotHave() throws IOException {
    final Path geowave = classpathJar("geowave-core-store-3.0.0-SNAPSHOT.jar");
    final Path olderGuava = classpathJar("guava-30.1-jre.jar");
    final Path gtMain = classpathJar("gt-main-35.1.jar");
    final Path hbaseStore = classpathJar("geowave-datastore-hbase-3.0.0-SNAPSHOT.jar");
    final Path classes = tempFolder.newFolder("classes").toPath();
    final String classpath =
        classpath(
            geowave,
            olderGuava,
            gtMain,
            classpathJar("jetty-server-12.1.12.jar"),
            classpathJar("jetty-ee11-webapp-12.1.12.jar"),
            classpathJar("jakarta.servlet-api-6.1.0.jar"),
            classpathJar("javax.servlet-api-4.0.1.jar"),
            classpathJar("jakarta.ws.rs-api-3.1.0.jar"),
            classpathJar("jersey-container-servlet-3.1.12.jar"),
            classpathJar("hk2-locator-3.0.6.jar"),
            classpathJar("spark-core_2.13-4.0.1.jar"),
            classpathJar("scala-library-2.13.16.jar"),
            classpathJar("hbase-client-2.4.2.jar"),
            classpathJar("hadoop-common-3.1.2.jar"),
            classpathJar("protobuf-java-3.17.1.jar"),
            hbaseStore,
            classes,
            classpathDir.resolve("missing.jar"));

    final List<Path> added = new GeoServerWebAppContext(war, classpath).getGeoWaveClasspath();

    assertEquals(Arrays.asList(geowave, olderGuava, hbaseStore, classes), added);
  }

  @Test
  public void warLibrariesComeFirst() throws Exception {
    final Path olderGuava = jar(classpathDir.resolve("guava-30.1-jre.jar"), "which.txt", "geowave");
    final Path geowave =
        jar(
            classpathDir.resolve("geowave-core-store-3.0.0-SNAPSHOT.jar"),
            "geowave.txt",
            "geowave");
    final GeoServerWebAppContext context =
        new GeoServerWebAppContext(war, classpath(olderGuava, geowave));
    final Server server = new Server();
    server.setHandler(context);
    server.start();
    try {
      final ClassLoader classLoader = context.getClassLoader();
      assertEquals("war", read(classLoader.getResource("which.txt")));
      assertNotNull(classLoader.getResource("geowave.txt"));
      assertNotNull(classLoader.getResource("gt-main.txt"));
    } finally {
      server.stop();
    }
  }
}
