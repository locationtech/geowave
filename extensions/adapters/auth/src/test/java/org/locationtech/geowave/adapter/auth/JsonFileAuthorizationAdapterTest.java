/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.auth;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

public class JsonFileAuthorizationAdapterTest {

  @Test
  public void testBasic() throws MalformedURLException {
    final SecurityContext context = new SecurityContext() {

      /** */
      private static final long serialVersionUID = 1L;

      @Override
      public Authentication getAuthentication() {
        final Authentication auth = new UsernamePasswordAuthenticationToken("fred", "barney");
        return auth;
      }

      @Override
      public void setAuthentication(final Authentication arg0) {}
    };
    SecurityContextHolder.setContext(context);
    final File cwd = new File(".");
    final AuthorizationSPI authProvider =
        new JsonFileAuthorizationFactory().create(
            new URL("file://" + cwd.getAbsolutePath() + "/src/test/resources/jsonAuthfile.json"));
    assertTrue(Arrays.equals(new String[] {"1", "2", "3"}, authProvider.getAuthorizations()));
  }

  @Test
  public void testUserDetails() throws MalformedURLException {
    final UserDetails ud = new User("fred", "fred", new ArrayList<GrantedAuthority>());
    final SecurityContext context = new SecurityContext() {

      /** */
      private static final long serialVersionUID = 1L;

      @Override
      public Authentication getAuthentication() {
        final Authentication auth = new UsernamePasswordAuthenticationToken(ud, "barney");
        return auth;
      }

      @Override
      public void setAuthentication(final Authentication arg0) {}
    };
    SecurityContextHolder.setContext(context);
    final File cwd = new File(".");
    final AuthorizationSPI authProvider =
        new JsonFileAuthorizationFactory().create(
            new URL("file://" + cwd.getAbsolutePath() + "/src/test/resources/jsonAuthfile.json"));
    assertTrue(Arrays.equals(new String[] {"1", "2", "3"}, authProvider.getAuthorizations()));
  }
}
