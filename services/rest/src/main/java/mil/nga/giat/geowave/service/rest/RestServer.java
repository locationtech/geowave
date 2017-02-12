package mil.nga.giat.geowave.service.rest;

import org.reflections.Reflections;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;

public class RestServer
{

	public static void main(
			String[] args ) {
		for (Class<?> operation : new Reflections(
				"mil.nga.giat.geowave").getTypesAnnotatedWith(GeowaveOperation.class)) {
			System.out.println(pathFor(operation) + " " + operation.getSimpleName());
		}
	}

	public static String pathFor(
			Class<?> operation ) {
		if (operation == Object.class) {
			return "";
		}
		GeowaveOperation operationInfo = operation.getAnnotation(GeowaveOperation.class);
		return pathFor(operationInfo.parentOperation()) + "/" + operationInfo.name();
	}
}
