/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.utils.InstantiationUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The routes the REST services serve, sorted by path. */
public class RestRoutes {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestRoutes.class);

  private final List<RestRoute> routes;
  private final Map<String, RestRoute> routesByPath = new HashMap<>();

  public RestRoutes(final Collection<RestRoute> routes) {
    final List<RestRoute> sorted = new ArrayList<>(routes);
    Collections.sort(sorted);
    this.routes = Collections.unmodifiableList(sorted);
    for (final RestRoute route : sorted) {
      routesByPath.putIfAbsent(route.getPath(), route);
    }
  }

  /** @return a route for every concrete {@link ServiceEnabledCommand} on the classpath */
  public static RestRoutes find() {
    final List<RestRoute> routes = new ArrayList<>();
    for (final Class<? extends ServiceEnabledCommand> operation : new Reflections(
        "org.locationtech.geowave").getSubTypesOf(ServiceEnabledCommand.class)) {
      if (!Modifier.isAbstract(operation.getModifiers())) {
        try {
          routes.add(new RestRoute(InstantiationUtils.newInstance(operation)));
        } catch (InstantiationException | IllegalAccessException e) {
          LOGGER.error("Unable to instantiate Service Resource", e);
        }
      }
    }
    return new RestRoutes(routes);
  }

  public List<RestRoute> list() {
    return routes;
  }

  /** @return the route with exactly this path, or null */
  public RestRoute get(final String path) {
    return routesByPath.get(path);
  }
}
