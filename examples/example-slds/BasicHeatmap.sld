<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0"
	xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
	xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc"
	xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	<NamedLayer>
		<Name>Heatmap</Name>
		<UserStyle>
			<Title>Heatmap</Title>
			<Abstract>A heatmap surface showing point density</Abstract>
			<FeatureTypeStyle>
				<Transformation>
					<ogc:Function name="gs:Heatmap">
						<ogc:Function name="parameter">
							<ogc:Literal>data</ogc:Literal>
						</ogc:Function>
						<ogc:Function name="parameter">
							<ogc:Literal>weightAttr</ogc:Literal>
							<ogc:Literal>Percentile</ogc:Literal>
						</ogc:Function>
						<ogc:Function name="parameter">
							<ogc:Literal>maxLevel</ogc:Literal>
							<ogc:Literal>9</ogc:Literal>
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
					<RasterSymbolizer>
						<!-- specify geometry attribute to pass validation -->
						<Geometry>
							<ogc:PropertyName>the_geom</ogc:PropertyName>
						</Geometry>
						<Opacity>0.75</Opacity>
						<ColorMap type="ramp">
							<ColorMapEntry color="#0000FF" quantity="0" label="No Data"
								opacity="0" />
							<ColorMapEntry color="#0000FF" quantity="0.1"
								label="No Data" opacity="0" />
							<ColorMapEntry color="#00007F" quantity="0.3"
								label="Lowest" />
							<ColorMapEntry color="#0000FF" quantity="0.4"
								label="Very Low" />
							<ColorMapEntry color="#00FFFF" quantity="0.55"
								label="Low" />
							<ColorMapEntry color="#00FF00" quantity="0.7"
								label="Medium-Low" />
							<ColorMapEntry color="#FFFF00" quantity="0.82"
								label="Medium" />
							<ColorMapEntry color="#FF7F00" quantity="0.93"
								label="Medium-High" />
							<ColorMapEntry color="#FF0000" quantity="0.97"
								label="High" />
							<ColorMapEntry color="#7FFF00" quantity="0.995"
								label="Very High" />
							<ColorMapEntry color="#FFFFFF" quantity="1.0"
								label="Highest" />
						</ColorMap>
					</RasterSymbolizer>
				</Rule>
			</FeatureTypeStyle>
		</UserStyle>
	</NamedLayer>
</StyledLayerDescriptor>