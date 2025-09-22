/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.auth;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Use the user details to to determine a user's name. Given the user's name, lookup the user
 * credentials in a Json file. The location of the file is provided through the URL (protocol is
 * file).
 */
public class JsonFileAuthorizationProvider implements AuthorizationSPI {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonFileAuthorizationProvider.class);

  private AuthorizationSet authorizationSet;


  public JsonFileAuthorizationProvider(final URL location) {
    if (location == null) {
      authorizationSet = new AuthorizationSet();
    } else {
      String path = location.getPath();
      if (!location.getProtocol().equals("file")
          || (!new File(path).canRead() && !new File("." + path).canRead())) {
        LOGGER.error("Cannot find file " + location.toString());
        authorizationSet = new AuthorizationSet();
      }
      try {
        if (!new File(path).canRead()) {
          path = "." + path;
        }
        parse(new File(path));
      } catch (final IOException e) {
        LOGGER.error("Cannot parse file " + location.toString(), e);
        authorizationSet = new AuthorizationSet();
      }
    }
  }

  private void parse(final File file) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    authorizationSet = mapper.readValue(file, AuthorizationSet.class);
  }

  @Override
  public String[] getAuthorizations() {
    final Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    if (auth == null) {
      return new String[0];
    }
    final Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    String userName = principal.toString();
    if (principal instanceof UserDetails) {
      // most likely type of principal
      final UserDetails userDetails = (UserDetails) principal;
      userName = userDetails.getUsername();
    }
    final List<String> auths = authorizationSet.findAuthorizationsFor(userName);
    final String[] result = new String[auths.size()];
    auths.toArray(result);
    return result;
  }
}
