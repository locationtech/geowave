/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.core.cli.utils;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/** Utility methods relating to URLs, particularly validation focused */
public class URLUtils {
  private static final String HTTP = "http";
  private static final String HTTPS = "https";
  private static String[] schemes = {HTTP, HTTPS};

  public static String getUrl(String url) throws URISyntaxException, MalformedURLException {
    if (url != null) {
      if (isValidURL(url)) {
        return url;
      }
      final boolean valid = isValidScheme(url);

      if (!valid) {
        url = HTTP + "://" + url;
      }
      URI uri = new URI(url);
      if (uri.getScheme() == null) {
        uri = new URI(HTTP + "://" + url);
      }
      URL targetURL = uri.toURL();
      if (targetURL.getPort() == -1) {
        targetURL =
            new URL(
                targetURL.getProtocol(),
                targetURL.getHost(),
                targetURL.getDefaultPort(),
                // HP Fortify "Path Traversal" False Positive
                // User input is not used at any point to determine the
                // file path.
                // The information is hard code in a single location and
                // accessible
                // though this method.
                targetURL.getFile());
      }
      if (String.valueOf(targetURL.getPort()).endsWith("443")) {
        targetURL =
            new URL(
                HTTPS,
                targetURL.getHost(),
                targetURL.getPort(),
                // HP Fortify "Path Traversal" False Positive
                // User input is not used at any point to determine the
                // file path.
                // The information is hard code in a single location and
                // accessible
                // though this method.
                targetURL.getFile());
      }
      return targetURL.toString();
    }
    return url;
  }

  /**
   * Validate a URL to quickly check if it is in proper URL format
   *
   * @param url url to validate
   * @return true if valid, false otherwise
   */
  private static boolean isValidURL(final String url) {
    URL targetURL = null;
    try {
      targetURL = new URL(url);
    } catch (final MalformedURLException e) {
      return false;
    }

    try {
      targetURL.toURI();
    } catch (final URISyntaxException e) {
      return false;
    }
    return true;
  }

  private static boolean isValidScheme(final String url) {
    final int ix = url.indexOf("://");
    if (ix == -1) {
      return false;
    }

    final String inputScheme = url.substring(0, ix);

    for (final String scheme : getSchemes()) {
      if (inputScheme.equalsIgnoreCase(scheme)) {
        return true;
      }
    }

    return false;
  }

  /** @return the schemes */
  public static String[] getSchemes() {
    return schemes;
  }

  /** @param schemes the schemes to set */
  public static void setSchemes(final String[] schemes) {
    URLUtils.schemes = schemes;
  }
}
