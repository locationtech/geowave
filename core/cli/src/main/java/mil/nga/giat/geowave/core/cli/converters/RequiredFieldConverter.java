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
/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

/**
 * This converter does nothing other than ensure that a required field is setup.
 * Using this - over the standard JCommander 'required=true' - allows a user to
 * be prompted for the field, rather than always throwing an error (i.e. a more
 * gracious way of reporting the error)
 *
 */
public class RequiredFieldConverter extends
		GeoWaveBaseConverter<String>
{

	public RequiredFieldConverter(
			String optionName ) {
		super(
				optionName);
	}

	@Override
	public String convert(
			String value ) {
		return value;
	}

	@Override
	public boolean isRequired() {
		return true;
	}
}
