/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.cli;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;
import redis.embedded.util.OsArchitecture;

public class RunRedisServerOptions {
  private static final Logger LOGGER = LoggerFactory.getLogger(RunRedisServerOptions.class);
  @Parameter(
      names = {"--port", "-p"},
      description = "Select the port for Redis to listen on (default is port 6379)")
  private Integer port = 6379;

  @Parameter(
      names = {"--directory", "-d"},
      description = "The directory to use for Redis. If set, the data will be persisted and durable. If none, it will use a temp directory and delete when complete")
  private String directory = null;
  @Parameter(
      names = {"--maxMemory", "-m"},
      description = "The maximum memory to use (in the form such as 512M or 1G)")
  private String memory = "1G";

  @Parameter(
      names = {"--setting", "-s"},
      description = "A setting to apply to Redis in the form of <name>=<value>")
  private List<String> settings = new ArrayList<>();

  public RedisServer getServer() throws IOException {
    final RedisServerBuilder bldr = RedisServer.builder().port(port).setting("bind 127.0.0.1"); // secure
    boolean appendOnlySet = false;
    for (final String s : settings) {
      final String[] kv = s.split("=");
      if (kv.length == 2) {
        if (kv[0].equalsIgnoreCase("appendonly")) {
          appendOnlySet = true;
        }
        bldr.setting(kv[0] + " " + kv[1]);
      }
    }
    if ((directory != null) && (directory.trim().length() > 0)) {
      RedisExecProvider execProvider = RedisExecProvider.defaultProvider();
      final File f = execProvider.get();

      final File directoryFile = new File(directory);
      if (!directoryFile.exists() && !directoryFile.mkdirs()) {
        LOGGER.warn("Unable to create directory '" + directory + "'");
      }

      final File newExecFile = new File(directoryFile, f.getName());
      boolean exists = false;
      if (newExecFile.exists()) {
        if (newExecFile.length() != f.length()) {
          if (!newExecFile.delete()) {
            LOGGER.warn("Unable to delete redis exec '" + newExecFile.getAbsolutePath() + "'");
          }
        } else {
          exists = true;
        }
      }
      if (!exists) {
        FileUtils.moveFile(f, newExecFile);
      }
      if (!appendOnlySet) {
        bldr.setting("appendonly yes");
        bldr.setting("appendfsync everysec");
      }

      final OsArchitecture osArch = OsArchitecture.detect();
      execProvider.override(osArch.os(), osArch.arch(), newExecFile.getAbsolutePath());
      bldr.redisExecProvider(execProvider);
    }
    bldr.setting("maxmemory " + memory.trim());
    return bldr.build();
  }
}
