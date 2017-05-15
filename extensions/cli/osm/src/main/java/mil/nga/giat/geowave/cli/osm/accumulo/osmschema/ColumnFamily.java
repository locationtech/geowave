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
package mil.nga.giat.geowave.cli.osm.accumulo.osmschema;

/**
 * Created by bennight on 1/31/2015.
 */
public class ColumnFamily
{
	public static final byte[] NODE = "n".getBytes(Constants.CHARSET);
	public static final byte[] WAY = "w".getBytes(Constants.CHARSET);
	public static final byte[] RELATION = "r".getBytes(Constants.CHARSET);
}
