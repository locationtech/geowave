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
package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import java.lang.reflect.AnnotatedElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.annotations.CentroidParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ClusteringParameter;
import mil.nga.giat.geowave.analytic.param.annotations.CommonParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.GlobalParameter;
import mil.nga.giat.geowave.analytic.param.annotations.HullParameter;
import mil.nga.giat.geowave.analytic.param.annotations.InputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.JumpParameter;
import mil.nga.giat.geowave.analytic.param.annotations.MapReduceParameter;
import mil.nga.giat.geowave.analytic.param.annotations.OutputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.PartitionParameter;
import mil.nga.giat.geowave.analytic.param.annotations.SampleParameter;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.prefix.TranslationEntry;

/**
 * This is a stop-gap measure which allows us to copy parameters read from the
 * command line into the PropertyManagement object.
 */
public class PropertyManagementConverter
{
	final static Logger LOGGER = LoggerFactory.getLogger(PropertyManagementConverter.class);

	final PropertyManagement properties;

	public PropertyManagementConverter(
			PropertyManagement properties ) {
		this.properties = properties;
	}

	public PropertyManagement getProperties() {
		return properties;
	}

	/**
	 * Find annotations in the object, and copy the values to the
	 * PropertyManagement
	 * 
	 * @param object
	 */
	public void readProperties(
			Object object ) {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(object);
		JCommanderTranslationMap map = translator.translate();
		for (TranslationEntry entry : map.getEntries().values()) {
			// Has annotation?
			AnnotatedElement element = entry.getMember();
			CentroidParameter centroid = element.getAnnotation(CentroidParameter.class);
			ClusteringParameter clustering = element.getAnnotation(ClusteringParameter.class);
			CommonParameter common = element.getAnnotation(CommonParameter.class);
			ExtractParameter extract = element.getAnnotation(ExtractParameter.class);
			GlobalParameter global = element.getAnnotation(GlobalParameter.class);
			HullParameter hull = element.getAnnotation(HullParameter.class);
			InputParameter input = element.getAnnotation(InputParameter.class);
			JumpParameter jump = element.getAnnotation(JumpParameter.class);
			MapReduceParameter mapReduce = element.getAnnotation(MapReduceParameter.class);
			OutputParameter output = element.getAnnotation(OutputParameter.class);
			PartitionParameter partition = element.getAnnotation(PartitionParameter.class);
			SampleParameter sample = element.getAnnotation(SampleParameter.class);

			if (centroid != null) {
				handleEnum(
						entry,
						centroid.value());
			}
			if (clustering != null) {
				handleEnum(
						entry,
						clustering.value());
			}
			if (common != null) {
				handleEnum(
						entry,
						common.value());
			}
			if (extract != null) {
				handleEnum(
						entry,
						extract.value());
			}
			if (global != null) {
				handleEnum(
						entry,
						global.value());
			}
			if (hull != null) {
				handleEnum(
						entry,
						hull.value());
			}
			if (input != null) {
				handleEnum(
						entry,
						input.value());
			}
			if (jump != null) {
				handleEnum(
						entry,
						jump.value());
			}
			if (mapReduce != null) {
				handleEnum(
						entry,
						mapReduce.value());
			}
			if (output != null) {
				handleEnum(
						entry,
						output.value());
			}
			if (partition != null) {
				handleEnum(
						entry,
						partition.value());
			}
			if (sample != null) {
				handleEnum(
						entry,
						sample.value());
			}

		}
	}

	/**
	 * For a single value, copy the value from the object to PropertyManagement.
	 * 
	 * @param entry
	 * @param enumVal
	 */
	@SuppressWarnings("unchecked")
	private void handleEnum(
			TranslationEntry entry,
			ParameterEnum<?>[] enumVals ) {
		Object value = entry.getParam().get(
				entry.getObject());
		if (value != null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format(
						"Analytic Property Value: %s = %s",
						entry.getAsPropertyName(),
						value.toString()));
			}
			for (ParameterEnum<?> enumVal : enumVals) {
				((ParameterEnum<Object>) enumVal).getHelper().setValue(
						properties,
						value);
			}
		}
	}
}
