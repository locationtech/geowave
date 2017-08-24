<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0"
	xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
	xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc"
	xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	<NamedLayer>
		<Name>Subsample At Requested Map Resolution</Name>
		<UserStyle>
			<Title>Subsample</Title>
			<Abstract>An example of how to handle large datasets in a WMS request
				by subsampling the data within GeoWave based on the pixel
				resolution.</Abstract>
			<FeatureTypeStyle>
				<Transformation>
					<ogc:Function name="geowave:Subsample">
						<ogc:Function name="parameter">
							<ogc:Literal>data</ogc:Literal>
						</ogc:Function>
						<ogc:Function name="parameter">
							<ogc:Literal>pixelSize</ogc:Literal>
							<ogc:Literal>1.5</ogc:Literal>
						</ogc:Function>
						<ogc:Function name="parameter">
							<ogc:Literal>outputBBOX</ogc:Literal>
							<ogc:Function name="env">
								<ogc:Literal>wms_bbox</ogc:Literal>
							</ogc:Function>
						</ogc:Function>
						<ogc:Function name="parameter">
							<ogc:Literal>outputWidth</ogc:Literal>
							<ogc:Function name="env">
								<ogc:Literal>wms_width</ogc:Literal>
							</ogc:Function>
						</ogc:Function>
						<ogc:Function name="parameter">
							<ogc:Literal>outputHeight</ogc:Literal>
							<ogc:Function name="env">
								<ogc:Literal>wms_height</ogc:Literal>
							</ogc:Function>
						</ogc:Function>
					</ogc:Function>
				</Transformation>
				<Rule>
					<!--Here you can put any style rules you want, unrelated to Subsampling, 
						this point styling merely serves as an example -->
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
							<Size>3</Size>
						</Graphic>
					</PointSymbolizer>
				</Rule>
			</FeatureTypeStyle>
		</UserStyle>
	</NamedLayer>
</StyledLayerDescriptor>