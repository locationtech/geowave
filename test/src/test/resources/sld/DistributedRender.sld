<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0"
	xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
	xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc"
	xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	<!-- a Named Layer is the basic building block of an SLD document -->
	<NamedLayer>
		<Name>distributed_render_line</Name>
		<UserStyle>
			<!-- Styles can have names, titles and abstracts -->
			<Title>Default Line with GeoWave Distributed Rendering enabled</Title>
			<Abstract>A sample style that draws a line using GeoWave's
				distributed rendering</Abstract>
			<FeatureTypeStyle>
				<Transformation>
					<ogc:Function name="geowave:DistributedRender">
						<ogc:Function name="parameter">
							<ogc:Literal>data</ogc:Literal>
						</ogc:Function>
					</ogc:Function>
				</Transformation>

				<Rule>
					<Name>Basic Red Square</Name>
					<Title>Red Square</Title>
					<Abstract>A 3 pixel square with a red fill and no stroke</Abstract>
					<PointSymbolizer>
						<Graphic>
							<Mark>
								<WellKnownName>square</WellKnownName>
								<Fill>
									<CssParameter name="fill">#FF0000</CssParameter>
								</Fill>
							</Mark>
							<Size>6</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
			</FeatureTypeStyle>
		</UserStyle>
	</NamedLayer>
</StyledLayerDescriptor>