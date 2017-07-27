/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.cli.parser;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.prefix.PrefixedJCommander;

public class CommandLineOperationParams implements
		OperationParams
{
	private final Map<String, Object> context = new HashMap<String, Object>();
	private final Map<String, Operation> operationMap = new LinkedHashMap<String, Operation>();
	private final String[] args;
	private PrefixedJCommander commander;
	private boolean validate = true;
	private boolean allowUnknown = false;
	private boolean commandPresent;
	private int successCode = 0;
	private String successMessage;
	private Throwable successException;

	public CommandLineOperationParams(
			String[] args ) {
		this.args = args;
	}

	public String[] getArgs() {
		return this.args;
	}

	/**
	 * Implement parent interface to retrieve operations
	 */
	@Override
	public Map<String, Operation> getOperationMap() {
		return operationMap;
	}

	@Override
	public Map<String, Object> getContext() {
		return this.context;
	}

	public PrefixedJCommander getCommander() {
		return this.commander;
	}

	public void setValidate(
			boolean validate ) {
		this.validate = validate;
	}

	public void setAllowUnknown(
			boolean allowUnknown ) {
		this.allowUnknown = allowUnknown;
	}

	public boolean isValidate() {
		return validate;
	}

	public boolean isAllowUnknown() {
		return this.allowUnknown;
	}

	public void setCommander(
			PrefixedJCommander commander ) {
		this.commander = commander;
	}

	public void addOperation(
			String name,
			Operation operation,
			boolean isCommand ) {
		commandPresent |= isCommand;
		this.operationMap.put(
				name,
				operation);
	}

	public boolean isCommandPresent() {
		return commandPresent;
	}

	public int getSuccessCode() {
		return successCode;
	}

	public void setSuccessCode(
			int successCode ) {
		this.successCode = successCode;
	}

	public String getSuccessMessage() {
		return successMessage;
	}

	public void setSuccessMessage(
			String successMessage ) {
		this.successMessage = successMessage;
	}

	public Throwable getSuccessException() {
		return successException;
	}

	public void setSuccessException(
			Throwable successException ) {
		this.successException = successException;
	}
}
