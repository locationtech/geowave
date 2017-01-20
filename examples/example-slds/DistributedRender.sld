<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0"
	xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
	xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc"
	xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	<!-- a Named Layer is the basic building block of an SLD document -->
	<NamedLayer>
		<Name>Distributed Render - Blue Line</Name>
		<UserStyle>
			<!-- Styles can have names, titles and abstracts -->
			<Title>Default Line with GeoWave Distributed Rendering enabled
			</Title>
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

					<!--Here you can put any style rules you want, unrelated to DistributedRendering, 
						this line styling merely serves as an example -->
					<Name>rule1</Name>
					<Title>Blue Line</Title>
					<Abstract>A solid blue line with a 1 pixel width</Abstract>
					<LineSymbolizer>
						<Stroke>
							<CssParameter name="stroke">#0000FF</CssParameter>
						</Stroke>
					</LineSymbolizer>
				</Rule>
			</FeatureTypeStyle>
		</UserStyle>
	</NamedLayer>
</StyledLayerDescriptor>