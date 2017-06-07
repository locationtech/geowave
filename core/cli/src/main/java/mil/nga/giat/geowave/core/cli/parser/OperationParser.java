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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.api.Operation;
import mil.nga.giat.geowave.core.cli.prefix.PrefixedJCommander;
import mil.nga.giat.geowave.core.cli.prefix.PrefixedJCommander.PrefixedJCommanderInitializer;
import mil.nga.giat.geowave.core.cli.spi.OperationEntry;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class OperationParser
{
	private final OperationRegistry registry;
	private final Set<Object> additionalObjects = new HashSet<Object>();

	public OperationParser(
			OperationRegistry registry ) {
		this.registry = registry;
	}

	public OperationParser() {
		this(
				OperationRegistry.getInstance());
	}

	/**
	 * Parse command line arguments into the given operation. The operation will
	 * be prepared, and then can be directly executed, or modified before being
	 * executed.
	 * 
	 * @param operation
	 * @param args
	 * @return
	 */
	public CommandLineOperationParams parse(
			Operation operation,
			String[] args ) {
		CommandLineOperationParams params = new CommandLineOperationParams(
				args);
		OperationEntry topLevelEntry = registry.getOperation(operation.getClass());
		// Populate the operation map.
		params.getOperationMap().put(
				topLevelEntry.getOperationName(),
				operation);
		parseInternal(
				params,
				topLevelEntry);
		return params;
	}

	/**
	 * Search the arguments for the list of commands/operations to execute based
	 * on the top level operation entry given.
	 * 
	 * @param topLevel
	 * @param args
	 * @return
	 */
	public CommandLineOperationParams parse(
			Class<? extends Operation> topLevel,
			String[] args ) {
		CommandLineOperationParams params = new CommandLineOperationParams(
				args);
		OperationEntry topLevelEntry = registry.getOperation(topLevel);
		parseInternal(
				params,
				topLevelEntry);
		return params;
	}

	/**
	 * Parse, starting from the given entry.
	 * 
	 * @param params
	 */
	private void parseInternal(
			CommandLineOperationParams params,
			OperationEntry topLevelEntry ) {

		try {
			PrefixedJCommander pluginCommander = new PrefixedJCommander();
			pluginCommander.setInitializer(new OperationContext(
					topLevelEntry,
					params));
			params.setCommander(pluginCommander);
			for (Object obj : additionalObjects) {
				params.getCommander().addPrefixedObject(
						obj);
			}

			// Parse without validation so we can prepare.
			params.getCommander().setAcceptUnknownOptions(
					true);
			params.getCommander().setValidate(
					false);
			params.getCommander().parse(
					params.getArgs());

			// Prepare stage:
			for (Operation operation : params.getOperationMap().values()) {
				// Do not continue
				if (!operation.prepare(params)) {
					params.setSuccessCode(1);
					return;
				}
			}

			// Parse with validation
			PrefixedJCommander finalCommander = new PrefixedJCommander();
			finalCommander.setInitializer(new OperationContext(
					topLevelEntry,
					params));
			params.setCommander(finalCommander);
			for (Object obj : additionalObjects) {
				params.getCommander().addPrefixedObject(
						obj);
			}
			params.getCommander().setAcceptUnknownOptions(
					params.isAllowUnknown());
			params.getCommander().setValidate(
					params.isValidate());
			params.getCommander().parse(
					params.getArgs());
		}
		catch (ParameterException p) {
			params.setSuccessCode(-1);
			params.setSuccessMessage("Error: " + p.getMessage());
			params.setSuccessException(p);
		}

		return;
	}

	/**
	 * Parse the command line arguments into the objects given in the
	 * 'additionalObjects' array. I don't really ever forsee this ever being
	 * used, but hey, why not.
	 * 
	 * @param args
	 */
	public CommandLineOperationParams parse(
			String[] args ) {

		CommandLineOperationParams params = new CommandLineOperationParams(
				args);

		try {
			PrefixedJCommander pluginCommander = new PrefixedJCommander();
			params.setCommander(pluginCommander);
			for (Object obj : additionalObjects) {
				params.getCommander().addPrefixedObject(
						obj);
			}
			params.getCommander().parse(
					params.getArgs());

		}
		catch (ParameterException p) {
			params.setSuccessCode(-1);
			params.setSuccessMessage("Error: " + p.getMessage());
			params.setSuccessException(p);
		}

		return params;
	}

	public Set<Object> getAdditionalObjects() {
		return additionalObjects;
	}

	public void addAdditionalObject(
			Object obj ) {
		additionalObjects.add(obj);
	}

	public OperationRegistry getRegistry() {
		return registry;
	}

	/**
	 * This class is used to lazily init child commands only when they are
	 * actually referenced/used by command line options. It will set itself on
	 * the commander, and then add its children as commands.
	 */
	public class OperationContext implements
			PrefixedJCommanderInitializer
	{

		private final OperationEntry operationEntry;
		private final CommandLineOperationParams params;
		private Operation operation;

		public OperationContext(
				OperationEntry entry,
				CommandLineOperationParams params ) {
			this.operationEntry = entry;
			this.params = params;
		}

		@Override
		public void initialize(
				PrefixedJCommander commander ) {
			commander.setCaseSensitiveOptions(false);

			// Add myself.
			if (params.getOperationMap().containsKey(
					operationEntry.getOperationName())) {
				operation = params.getOperationMap().get(
						operationEntry.getOperationName());
			}
			else {
				operation = operationEntry.createInstance();
				params.addOperation(
						operationEntry.getOperationName(),
						operation,
						operationEntry.isCommand());
			}
			commander.addPrefixedObject(operation);

			// initialize the commander by adding child operations.
			for (OperationEntry child : operationEntry.getChildren()) {
				commander.addCommand(
						child.getOperationName(),
						null);
			}

			// Update each command to add an initializer.
			Map<String, JCommander> childCommanders = commander.getCommands();
			for (OperationEntry child : operationEntry.getChildren()) {
				PrefixedJCommander pCommander = (PrefixedJCommander) childCommanders.get(child.getOperationName());
				pCommander.setInitializer(new OperationContext(
						child,
						params));
			}
		}

		public Operation getOperation() {
			return operation;
		}

		public OperationEntry getOperationEntry() {
			return operationEntry;
		}
	}
}
