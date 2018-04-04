package mil.nga.giat.geowave.service.grpc;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.text.WordUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.service.grpc.GeoWaveGrpcOperationParser;

public class GeowaveOperationGrpcGenerator
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeowaveOperationGrpcGenerator.class.getName());
	private static final String protobufPackage = "option java_package = \"mil.nga.giat.geowave.service.grpc.protobuf\";\n";
	private static final String header = "/*******************************************************************************\n"
			+ " * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation\n"
			+ " *\n"
			+ " * See the NOTICE file distributed with this work for additional\n"
			+ " * information regarding copyright ownership.\n"
			+ " * All rights reserved. This program and the accompanying materials\n"
			+ " * are made available under the terms of the Apache License\n"
			+ " * Version 2.0 which accompanies this distribution and is available at\n"
			+ " * http://www.apache.org/licenses/LICENSE-2.0.txt\n"
			+ " ******************************************************************************/\n"
			+ "syntax = \"proto3\";\n";

	private static final String options = "option java_multiple_files = true;\n" + protobufPackage
			+ "option java_outer_classname = \"&OUTER_CLASSNAME&\";\n";

	private static String outputBasePath = "";

	public static void main(
			final String[] args ) {

		if (args.length > 0) outputBasePath = args[0];

		GeowaveOperationGrpcGenerator g = new GeowaveOperationGrpcGenerator();
		try {
			g.parseOperationsForApiRoutes();
		}
		catch (NoSuchMethodException | SecurityException e) {
			LOGGER.error(
					"Exception encountered parsing operations",
					e);
		}
	}

	/**
	 * This method parses all the Geowave Operation classes and creates the info
	 * to generate a gRPC based on the operation.
	 * 
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 */
	public void parseOperationsForApiRoutes()
			throws NoSuchMethodException,
			SecurityException {

		final HashMap<String, ArrayList<String>> rpcs = new HashMap<String, ArrayList<String>>();
		final HashMap<String, ArrayList<String>> rpcInputMessages = new HashMap<String, ArrayList<String>>();
		final HashMap<String, String> retMessages = new HashMap<String, String>();

		Set<Class<? extends ServiceEnabledCommand>> t = null;
		try {
			t = new Reflections(
					"mil.nga.giat.geowave").getSubTypesOf(ServiceEnabledCommand.class);
		}
		catch (Exception e) {
			LOGGER.debug(e.getMessage());
		}

		if (t == null) {
			LOGGER.debug("No operations found");
			return;
		}

		for (final Class<? extends ServiceEnabledCommand> operation : t) {

			if (!Modifier.isAbstract(operation.getModifiers())) {
				// Tokenize the package name so we can store the operations
				// according to their original package names
				final String packageName = operation.getPackage().getName();
				final String[] packageToks = packageName.split("\\.");
				String serviceName = "";
				for (int i = 0; i < packageToks.length; i++) {
					if (packageToks[i].equalsIgnoreCase("geowave")) {
						// this special case is specifically for CoreMapreduce
						// (which is packaged as ..geowave.mapreduce for some
						// reason)
						if (packageToks[i + 2].equalsIgnoreCase("operations"))
							serviceName = "Core" + WordUtils.capitalize(packageToks[i + 1]);
						else
							serviceName = WordUtils.capitalize(packageToks[i + 1])
									+ WordUtils.capitalize(packageToks[i + 2]);
						if (!rpcs.containsKey(serviceName)) {
							rpcs.put(
									serviceName,
									new ArrayList<String>());
							rpcInputMessages.put(
									serviceName,
									new ArrayList<String>());
							break;
						}
					}
				}

				LOGGER.info("Parsing operation: " + operation.getName());

				// tokenize the operation name so we can generate a name for
				// the RPC
				final String[] rpcNameToks = operation.getName().split(
						"\\.");
				final String rpcName = rpcNameToks[rpcNameToks.length - 1];

				// get the return type for this command
				String responseName = "";

				Class<?> parentClass = operation;
				boolean success = false;
				Type paramType = null;
				while (parentClass != null) {

					try {
						paramType = ((ParameterizedType) parentClass.getGenericSuperclass()).getActualTypeArguments()[0];
						success = true;
					}
					catch (final Exception e) {
						continue;
					}
					finally {
						if (success) break;
						parentClass = parentClass.getSuperclass();
					}

				}

				if (success) {
					String retType = GeoWaveGrpcOperationParser.getGrpcReturnType(paramType.getTypeName());
					responseName = retType.replaceAll(
							"(<)|(>)|(,)",
							" ");
					responseName = WordUtils.capitalize(responseName);
					responseName = responseName.replaceAll(
							" ",
							"") + "Response";
					// if the return type is void we need to return an
					// empty message
					if (retType.equalsIgnoreCase("void"))
						retType = "\nmessage " + responseName + " { }";
					else
						retType = "\nmessage " + responseName + " { " + retType + " responseValue = 1; }";
					retMessages.put(
							retType,
							retType);
				}

				final String rpc = "\t rpc " + rpcName + "(" + rpcName + "Parameters) returns (" + responseName
						+ ") {} \n";
				rpcs.get(
						serviceName).add(
						rpc);
				final ProcessOperationResult pr = new ProcessOperationResult();
				pr.message = "\nmessage " + rpcName + "Parameters {";
				pr.currFieldPosition = 1;

				Class<?> opClass = operation;
				try {
					while (opClass.getSuperclass() != null) {
						processOperation(
								opClass,
								pr);
						opClass = opClass.getSuperclass();
					}
				}
				catch (final IOException e) {
					LOGGER.error(
							"Exception encountered processing operations",
							e);
				}
				pr.message += "\n}\n";
				rpcInputMessages.get(
						serviceName).add(
						pr.message);
			}
		}

		// write out all the service files
		Iterator it = rpcs.entrySet().iterator();
		while (it.hasNext()) {
			HashMap.Entry pair = (HashMap.Entry) it.next();
			final String currServiceName = (String) pair.getKey();
			ArrayList<String> rpcList = (ArrayList<String>) pair.getValue();
			ArrayList<String> rpcInputMessageList = (ArrayList<String>) rpcInputMessages.get(currServiceName);

			String serviceFilename = outputBasePath + "/src/main/protobuf/GeoWave" + pair.getKey() + ".proto";
			// "../protobuf-shaded/src/main/protobuf/GeoWave" + pair.getKey() +
			// ".proto";
			Writer serviceWriter = null;
			try {
				serviceWriter = new OutputStreamWriter(
						new FileOutputStream(
								serviceFilename),
						"UTF-8");
			}
			catch (final IOException e) {
				LOGGER.error(
						"Exception encountered opening file stream",
						e);
			}

			// first write header
			final String serviceHeader = header + "import \"GeoWaveReturnTypes.proto\";\n" + options.replace(
					"&OUTER_CLASSNAME&",
					currServiceName + "Service");
			try {
				if (serviceWriter != null) {
					serviceWriter.write(serviceHeader + "\n");

					// write out service definition
					serviceWriter.write("service " + currServiceName + " { \n");

					// write out rpcs for this service
					for (int i = 0; i < rpcList.size(); i++) {
						serviceWriter.write(rpcList.get(i));
					}

					// end service definition
					serviceWriter.write("}\n");

					for (int i = 0; i < rpcInputMessageList.size(); i++) {
						serviceWriter.write(rpcInputMessageList.get(i));
					}
				}
			}
			catch (final IOException e) {
				LOGGER.error(
						"Exception encountered writing proto file",
						e);
			}
			finally {
				safeClose(serviceWriter);
			}

		}

		String serviceReturnFilename = outputBasePath + "/src/main/protobuf/GeoWaveReturnTypes.proto";
		Writer serviceReturnWriter = null;
		try {
			serviceReturnWriter = new OutputStreamWriter(
					new FileOutputStream(
							serviceReturnFilename),
					"UTF-8");
		}
		catch (final IOException e) {
			LOGGER.error(
					"Exception encountered opening file stream",
					e);
		}

		try {
			// write out proto file for the service return types
			// this file is included/imported by all the service definition
			// files
			if (serviceReturnWriter != null) {
				serviceReturnWriter.write(header + protobufPackage);

				it = retMessages.entrySet().iterator();
				while (it.hasNext()) {
					HashMap.Entry pair = (HashMap.Entry) it.next();
					serviceReturnWriter.write((String) pair.getValue());
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Exception encountered writing proto file",
					e);
		}
		finally {
			safeClose(serviceReturnWriter);
		}

	}

	public String processOperation(
			final Class<?> operation,
			ProcessOperationResult pr )
			throws IOException {

		Field[] fields = operation.getDeclaredFields();

		for (int i = 0; i < fields.length; i++) {
			if (fields[i].isAnnotationPresent(Parameter.class)) {

				String type = GeoWaveGrpcOperationParser.getGrpcType(fields[i].getType());
				pr.message += "\n\t" + type;
				if (type.equalsIgnoreCase("repeated")) {
					ParameterizedType parameterizedType = (ParameterizedType) fields[i].getGenericType();
					Type actualType = parameterizedType.getActualTypeArguments()[0];
					pr.message += " " + GeoWaveGrpcOperationParser.getGrpcType(actualType.getClass());
				}
				pr.message += " " + fields[i].getName() + " = " + pr.currFieldPosition + ";";
				pr.currFieldPosition++;
			}

			if (fields[i].isAnnotationPresent(ParametersDelegate.class)) {
				processOperation(
						fields[i].getType(),
						pr);
			}
		}
		return "";
	}

	public static void safeClose(
			Writer writer ) {
		if (writer != null) {
			try {
				writer.close();
			}
			catch (IOException e) {
				LOGGER.error(
						"Encountered exception while trying to close file stream",
						e);
			}
		}
	}

	private static class ProcessOperationResult
	{
		String message;
		int currFieldPosition;
	}
}
