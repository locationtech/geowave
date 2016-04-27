package mil.nga.giat.geowave.cli.osm.osmfeature.types.attributes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttributeDefinition
{
	public String type = null;
	public String name = null;
	public String key = null;
	public final Map<String, List<String>> args = new HashMap<>();

	public Object convert(
			Object obj ) {
		return AttributeTypes.getAttributeType(
				type).convert(
				obj);
	}
}
