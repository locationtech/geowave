package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.FieldLevelVisibilityWriter;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Object defining visibility is a json structure where each attribute defines
 * the visibility for a field with the same name (as the attribute).
 * 
 * Example: { "geometry" : "S", "eventName": "TS"}
 * 
 * Json attributes can also be regular expressions, matching more than one field
 * name.
 * 
 * Example: { "geo.*" : "S", ".*" : "TS"}.
 * 
 * The order of the expression must be considered if one expression is more
 * general than another, as shown in the example. The expression ".*" matches
 * all attributes. The more specific expression "geo.*" must be ordered first.
 * 
 * 
 * 
 */
public class JsonDefinitionColumnVisibilityManagement<T> extends
		ColumnVisibilityManagement<T>
{

	private final static Logger LOGGER = Logger.getLogger(JsonDefinitionColumnVisibilityManagement.class);

	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public byte[] translateVisibility(
			Object visibilityObject,
			String fieldName ) {
		if (visibilityObject == null) return new byte[0];
		try {
			JsonNode attributeMap = mapper.readTree(visibilityObject.toString());
			JsonNode field = attributeMap.get(fieldName);
			if (field != null && field.isValueNode()) return validate(field.getTextValue());
			Iterator<String> attNameIt = attributeMap.getFieldNames();
			while (attNameIt.hasNext()) {
				String attName = attNameIt.next();
				if (fieldName.matches(attName)) {
					JsonNode attNode = attributeMap.get(attName);
					if (attNode == null) {
						LOGGER.error("Cannot parse visibility expression, JsonNode for attribute " + attName + " was null");
						return null;
					}
					return validate(attNode.getTextValue());
				}
			}
		}
		catch (IOException | NullPointerException e) {
			LOGGER.error(
					"Cannot parse visibility expression " + visibilityObject.toString(),
					e);
		}
		return null;
	}

	@Override
	public FieldVisibilityHandler<T, Object> createVisibilityHandler(
			String fieldName,
			FieldVisibilityHandler<T, Object> defaultHandler,
			String visibilityAttributeName ) {
		return new FieldLevelVisibilityHandler<T, Object>(
				fieldName,
				defaultHandler,
				visibilityAttributeName,
				this);
	}

	protected byte[] validate(
			String vis ) {
		try {
			ColumnVisibility cVis = new ColumnVisibility(
					vis);
			return cVis.getExpression();
		}
		catch (Exception ex) {
			LOGGER.error(
					"Failed to parse visibility " + vis,
					ex);
			return null;
		}
	}

	@Override
	public FieldLevelVisibilityWriter<T, Object> createVisibilityWriter(
			String fieldName,
			FieldWriter<T, Object> writer,
			FieldVisibilityHandler<T, Object> defaultFieldVisiblityHandler,
			String visibilityAttribute ) {
		// ignore the visibility attribute field
		if (fieldName.equals(visibilityAttribute)) return null;

		return new FieldLevelVisibilityWriter<T, Object>(
				fieldName,
				writer,
				defaultFieldVisiblityHandler,
				visibilityAttribute,
				this);
	}
}
