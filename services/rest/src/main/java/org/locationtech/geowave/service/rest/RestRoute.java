/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.service.rest;

import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;

/**
 * Holds necessary information to create a Restlet route
 */
public class RestRoute implements
		Comparable<RestRoute>
{
	private final String path;
	private final ServiceEnabledCommand<?> operation;

	/**
	 * Create a new route given an operation
	 *
	 * @param operation
	 */
	public RestRoute(
			final ServiceEnabledCommand<?> operation ) {
		path = operation.getPath();
		this.operation = operation;
	}

	/**
	 * Return the operation as it was originally passed
	 *
	 * @return
	 */
	public ServiceEnabledCommand<?> getOperation() {
		return operation;
	}

	/**
	 * Get the path that represents the route
	 *
	 * @return a string representing the path, specified by pathFor
	 */
	public String getPath() {
		return path;
	}

	@Override
	public int compareTo(
			final RestRoute route ) {
		return path.compareTo(route.path);
	}

	@Override
	public boolean equals(
			final Object route ) {
		return (route instanceof RestRoute) && path.equals(((RestRoute) route).path);
	}

	@Override
	public int hashCode() {
		return path.hashCode();
	}
}
