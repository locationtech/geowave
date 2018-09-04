package mil.nga.giat.geowave.service.grpc;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class GeoWaveGrpcOperationParser
{
	public void enumFields() {

	}

	public static String getGrpcType(
			final Class<?> type ) {
		// note: array and enum types require deeper handling and
		// thus should be processed outside this method as well
		if (type == String.class) {
			return "string";
		}
		else if ((type == Integer.class) || (type == int.class)) {
			return "int32";
		}
		else if ((type == Long.class) || (type == long.class)) {
			return "long";
		}
		else if ((type == Float.class) || (type == float.class)) {
			return "float";
		}
		else if ((type == Double.class) || (type == double.class)) {
			return "double";
		}
		else if ((type == Boolean.class) || (type == boolean.class)) {
			return "bool";
		}
		else if ((type != null) && ((Class<?>) type).isEnum()) {
			return "string";
			// TODO investigate this!
			// return "enum";
		}
		else if ((type == List.class)) {
			return "repeated";
		}
		else if ((type != null) && ((Class<?>) type).isArray()) {
			return "repeated " + getGrpcType(type.getComponentType());
		}
		return "string";
	}

	public static String getGrpcReturnType(
			final String type ) {
		// note: array and enum types require deeper handling and
		// thus should be processed outside this method as well
		String[] toks = type.split("(<)|(>)");
		String baseType = toks[0];

		if (baseType.equalsIgnoreCase(String.class.getTypeName())) {
			return "string";
		}
		else if (baseType.equalsIgnoreCase(Integer.class.getTypeName())
				|| baseType.equalsIgnoreCase(int.class.getTypeName())) {
			return "int32";
		}
		else if (baseType.equalsIgnoreCase(long.class.getTypeName())
				|| baseType.equalsIgnoreCase(Long.class.getTypeName())) {
			return "long";
		}
		else if (baseType.equalsIgnoreCase(Float.class.getTypeName())
				|| baseType.equalsIgnoreCase(float.class.getTypeName())) {
			return "float";
		}
		else if (baseType.equalsIgnoreCase(Double.class.getTypeName())
				|| baseType.equalsIgnoreCase(double.class.getTypeName())) {
			return "double";
		}
		else if (baseType.equalsIgnoreCase(Boolean.class.getTypeName())
				|| baseType.equalsIgnoreCase(boolean.class.getTypeName())) {
			return "bool";
		}
		else if (baseType.equalsIgnoreCase(List.class.getTypeName())) {
			return "repeated " + getGrpcReturnType(toks[1]);
		}
		else if (baseType.equalsIgnoreCase(SortedMap.class.getTypeName())
				|| baseType.equalsIgnoreCase(Map.class.getTypeName())) {
			toks[1] = toks[1].replaceAll(
					" ",
					"");
			String[] paramToks = toks[1].split(",");
			String grpcType = "map<" + getGrpcReturnType(paramToks[0]) + ", " + getGrpcReturnType(paramToks[1]) + ">";
			return grpcType;
		}
		else if (baseType.equalsIgnoreCase(Object.class.getTypeName())) {
			return "string";
		}
		return "void";
	}
}
