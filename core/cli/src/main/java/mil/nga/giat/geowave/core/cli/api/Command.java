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
package mil.nga.giat.geowave.core.cli.api;

/**
 * An operation may choose to implement Command, which will then lead to the
 * 'execute' method being called during the execute() phase.
 */
public interface Command extends
		Operation
{
	/**
	 * Execute the command, and return whether we want to continue execution
	 * 
	 * @return
	 */
	public void execute(
			OperationParams params )
			throws Exception;
}
