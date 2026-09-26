/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.security;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Keeps API keys in a SQLite database file, which needs org.xerial:sqlite-jdbc at runtime. */
public class SQLiteApiKeyStore implements ApiKeyStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteApiKeyStore.class);

  private final String url;

  /**
   * @param dbFile the database file, created if it does not exist
   * @throws IllegalStateException if the driver is missing or the database cannot be set up
   */
  public SQLiteApiKeyStore(final String dbFile) {
    url = "jdbc:sqlite:" + dbFile;
    try {
      // A driver in a webapp's WEB-INF/lib is not found by DriverManager's service loading.
      Class.forName("org.sqlite.JDBC");
    } catch (final ClassNotFoundException e) {
      throw new IllegalStateException(
          "API keys need the SQLite JDBC driver (org.xerial:sqlite-jdbc) on the classpath",
          e);
    }
    try (Connection conn = DriverManager.getConnection(url);
        Statement statement = conn.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS api_keys (\n"
              + "	id integer PRIMARY KEY,\n"
              + "	apiKey blob NOT NULL,\n"
              + "	username text NOT NULL\n"
              + ");");
    } catch (final SQLException e) {
      throw new IllegalStateException("Unable to set up the API key database " + dbFile, e);
    }
  }

  @Override
  public boolean hasKey(final String apiKey) {
    try (Connection conn = DriverManager.getConnection(url);
        PreparedStatement query = conn.prepareStatement("SELECT 1 FROM api_keys WHERE apiKey=?;")) {
      query.setString(1, apiKey);
      try (ResultSet rs = query.executeQuery()) {
        return rs.next();
      }
    } catch (final SQLException e) {
      LOGGER.error("Unable to look up an API key", e);
      return false;
    }
  }

  @Override
  public synchronized String getOrCreateKey(final String userName) {
    try (Connection conn = DriverManager.getConnection(url)) {
      try (PreparedStatement query =
          conn.prepareStatement("SELECT apiKey FROM api_keys WHERE username=?;")) {
        query.setString(1, userName);
        try (ResultSet rs = query.executeQuery()) {
          if (rs.next()) {
            return rs.getString("apiKey");
          }
        }
      }
      final String apiKey = UUID.randomUUID().toString();
      LOGGER.info("Inserting a new api key and user.");
      try (PreparedStatement insert =
          conn.prepareStatement("INSERT INTO api_keys (apiKey, username) VALUES(?, ?);")) {
        insert.setString(1, apiKey);
        insert.setString(2, userName);
        insert.executeUpdate();
      }
      return apiKey;
    } catch (final SQLException e) {
      LOGGER.error("Unable to read or store the API key of " + userName, e);
      return null;
    }
  }
}
