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
package mil.nga.giat.geowave.core.ingest.kafka;

public class KafkaCommandLineArgument
{
	private final String argName;
	private final String argDescription;
	private final String kafkaParamName;
	private final boolean required;

	public KafkaCommandLineArgument(
			final String argName,
			final String argDescription,
			final String kafkaParamName,
			final boolean required ) {
		this.argName = argName;
		this.argDescription = "See Kafka documention for '" + kafkaParamName + "'" + argDescription;
		this.kafkaParamName = kafkaParamName;
		this.required = required;
	}

	public String getArgName() {
		return argName;
	}

	public String getArgDescription() {
		return argDescription;
	}

	public String getKafkaParamName() {
		return kafkaParamName;
	}

	public boolean isRequired() {
		return required;
	}

}
