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
package org.locationtech.geowave.core.cli.api;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;

public abstract class ServiceEnabledCommand<T> extends
		DefaultOperation implements
		Command
{
	protected String path = null;

	abstract public T computeResults(
			OperationParams params )
			throws Exception;

	/**
	 * this method provides a hint to the service running the command whether it
	 * should be run asynchronously or not
	 * 
	 * @return should this method be run asynchronously
	 */
	public boolean runAsync() {
		return false;
	}

	/**
	 * the method to expose as a resource
	 *
	 * @return the HTTP method
	 */
	public HttpMethod getMethod() {
		final String path = getPath();
		if (path.contains("get") || path.contains("list")) {
			return HttpMethod.GET;
		}
		return HttpMethod.POST;
	}

	/**
	 * Get the status code to return if execution was success.
	 * 
	 * By default: POST -> 201 OTHER -> 200
	 * 
	 * Should be overridden in subclasses as needed (i.e., for a POST that does
	 * not create anything).
	 * 
	 * @return The potential status if REST call is successful.
	 */
	public Boolean successStatusIs200() {
		switch (getMethod()) {
			case POST:
				return false;
			default:
				return true;
		}
	}

	/**
	 * get the path to expose as a resource
	 *
	 * @return the path (use {param} for path encoded params)
	 */
	public String getPath() {
		if (path == null) {
			path = defaultGetPath();
		}
		return path.replace(
				"geowave",
				"v0");
	}

	public String getId() {
		return defaultId();
	}

	/**
	 * this is for ease if a class wants to merely override the final portion of
	 * a resource name and not the entire path
	 *
	 * @return the final portion of a resource name
	 */
	protected String getName() {
		return null;
	}

	private String defaultId() {
		// TODO this is used by swagger and it may determine layout but its
		// uncertain

		if (getClass().isAnnotationPresent(
				GeowaveOperation.class)) {
			final GeowaveOperation op = getClass().getAnnotation(
					GeowaveOperation.class);
			return op.parentOperation().getName() + "." + op.name();
		}
		else if ((getName() != null) && !getName().trim().isEmpty()) {
			return getName();
		}
		return getClass().getTypeName();
	}

	private String defaultGetPath() {
		final Class<?> operation = getClass();
		if (operation.isAnnotationPresent(GeowaveOperation.class)) {
			return pathFor(
					operation,
					getName()).substring(
					1);
		}
		else if ((getName() != null) && !getName().trim().isEmpty()) {
			return getName();
		}
		return operation.getTypeName();
	}

	/**
	 * Get the path for a command based on the operation hierarchy Return the
	 * path as a string in the format "/first/next/next"
	 *
	 * @param operation
	 *            - the operation to find the path for
	 * @return the formatted path as a string
	 */
	private static String pathFor(
			final Class<?> operation,
			final String resourcePathOverride ) {

		// Top level of hierarchy
		if (operation == Object.class) {
			return "";
		}

		final GeowaveOperation operationInfo = operation.getAnnotation(GeowaveOperation.class);
		return pathFor(
				operationInfo.parentOperation(),
				null) + "/" + resolveName(
				operationInfo.name(),
				resourcePathOverride);
	}

	private static String resolveName(
			final String operationName,
			final String resourcePathOverride ) {
		if ((resourcePathOverride == null) || resourcePathOverride.trim().isEmpty()) {
			return operationName;
		}
		return resourcePathOverride;
	}

	public static enum HttpMethod {
		GET,
		POST,
		PUT,
		PATCH,
		DELETE
	}
}
