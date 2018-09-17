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
package org.locationtech.geowave.service.rest.field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RequestParameters
{

	protected Map<String, Object> keyValuePairs;

	protected RequestParameters() {
		keyValuePairs = new HashMap<String, Object>();
	}

	/**
	 * Returns the specified parameter.
	 * 
	 * @param parameter
	 *            The key name of the desired value.
	 * @return The value of the specified key name.
	 */
	public Object getValue(
			String parameter ) {
		return keyValuePairs.get(parameter);
	}

	/**
	 * Returns the specified parameter, as a String. How the String is created
	 * depends on the implementation in the subclass of RequestParameters.
	 * 
	 * @param parameter
	 *            The key name of the desired value.
	 * @return The value of the specified key name, as a String.
	 */
	public abstract String getString(
			String parameter );

	/**
	 * Returns the specified parameter, as a List. How the List is created
	 * depends on the implementation in the subclass of RequestParameters.
	 * 
	 * @param parameter
	 *            The key name of the desired value.
	 * @return The value of the specified key name, as a List.
	 */
	public abstract List<?> getList(
			String parameter );

	/**
	 * Returns the specified parameter, as an Array. How the Array is created
	 * depends on the implementation in the subclass of RequestParameters.
	 * 
	 * @param parameter
	 *            The key name of the desired value.
	 * @return The value of the specified key name, as an Array.
	 */
	public abstract Object[] getArray(
			String parameter );

	/**
	 * Assumes the value of the parameter is a comma-delimited String, then
	 * returns an Array of String values based on the original String.
	 * 
	 * @param parameter
	 *            The key name of the desired value.
	 * @return an Array of Strings, parsed from the original String value.
	 */
	protected String[] splitStringParameter(
			String parameter ) {
		String value = getString(parameter);
		if (value == null) {
			return null;
		}
		return value.split(",");
	}
}
