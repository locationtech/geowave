---
layout: page
title: Screenshots
header: Screenshots
group: navigation
permalink: "screenshots.html"
---
{% include JB/setup %}


Screenshots below are of data loaded from the various attributed data sets into a Geowave instance, processed (in some cases) by a Geowave analytic process, and rendered by Geoserver.  <br/>


<div class="note info">
  <h5>T-Drive</h5>
  <p>
	Microsoft research has made available a trajectory data set that contains the GPS coordinates of 10,357 taxis in Beijing, China and surrounding areas over 	a one week period.<br/>
	There are approximately 15 million points in this data set.
  </p><p>
	More information on this data set is available at: 
	<a href="http://research.microsoft.com/apps/pubs/?id=152883" target="_blank">Microsoft Research T-drive page</a><br/>
  </p>
</div>

### T-drive at city scale
<p align="center">
	<a href="https://ngageoint.github.io/geowave/assets/images/t-drive-points-12-full.jpg" target="_blank"><img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-points-12-thumb.jpg" alt="T-drive points at city scale"></a><br/>
	<br/>
	<a href="https://ngageoint.github.io/geowave/assets/images/t-drive-density-12-full.jpg" target="_blank"><img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-density-12-thumb.jpg" alt="T-drive density at city scale"></a><br/>
	<br/>	
</p>

Above is the t-drive data set displaying both the raw points, as well as the results of a Geowave kernel density analytic.  The data corresponds to Mapbox zoom level 12.  

### T-drive at block scale
<p align="center">
	<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-points-2.jpg" alt="T-drive points at block scale"><br/>
	<i>Graphic background &copy;MapBox and &copy;OpenStreetMap<br/><br/>
	<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-density-2.jpg" alt="T-drive density at block scale"><br/>
	<i>Graphic background &copy;MapBox and &copy;OpenStreetMap<br/><br/>
</p>
This data set corresponds to a Mapbox zoom level of 15

### T-drive at house scale
<p align="center">
	<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-points-3.jpg" alt="T-drive points at building scale"><br/>
	<i>Graphic background &copy;MapBox and &copy;OpenStreetMap<br/><br/>
	<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-density-3.jpg" alt="T-drive density at building scale"><br/>
	<i>Graphic background &copy;MapBox and &copy;OpenStreetMap<br/><br/>
</p>
This data set corresponds to a Mapbox zoom level of 17



