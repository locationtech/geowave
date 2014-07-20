---
layout: page
title: Screenshots
header: Screenshots
group: navigation
permalink: "screenshots.html"
---
{% include JB/setup %}


Screenshots below are of data loaded from the various attributed data sets into a Geowave instance, processed (in some cases) by a Geowave analytic process, and rendered by Geoserver.  The background images are from Mapbox and are &copy;Mapbox as well as &copy;OpenStreeMap for the cartographic information.


# Microsoft Research T-drive
---
Microsoft research has made available a trajectory data set that contains the GPS coordinates of 10,357 taxis in Beijing, China and surrounding areas over a one week period.
There are approximately 15 million points in this data set.

### T-drive at city scale
<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-points-1.jpg" alt="T-drive points at city scale"><br/><br/>
<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-density-1.jpg" alt="T-drive density at city scale">

Above is the t-drive data set displaying both the raw points, as well as the results of a Geowave kernel density analytic.  The data corresponds to Mapbox zoom level 12.  

### T-drive at block scale
<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-points-2.jpg" alt="T-drive points at block scale"><br/><br/>
<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-density-2.jpg" alt="T-drive density at block scale">

This data set corresponds to a Mapbox zoom level of 15

### T-drive at house scale
<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-points-3.jpg" alt="T-drive points at building scale"><br/><br/>
<img align="center" src="https://ngageoint.github.io/geowave/assets/images/t-drive-density-3.jpg" alt="T-drive density at building scale">

This data set corresponds to a Mapbox zoom level of 17

<div class="note info">
  <h5>T-Drive: More Info</h5>
  <p>
	More information on this data set is available at: <br/>
	<a href="http://research.microsoft.com/apps/pubs/?id=152883">Microsoft Research T-drive page</a><br/><br/>
	or in the following papers:
	[1] Jing Yuan, Yu Zheng, Xing Xie, and Guangzhong Sun. Driving with knowledge from the physical world. <i>In The 17th ACM SIGKDD international conference on Knowledge Discovery and Data mining</i>, KDD'11, New York, NY, USA, 2011. ACM.<br/>
	[2] Jing Yuan, Yu Zheng, Chengyang Zhang, Wenlei Xie, Xing Xie, Guangzhong Sun, and Yan Huang. T-drive: driving directions based on taxi trajectories. <i>In Proceedings of the 18th SIGSPATIAL International Conference on Advances in Geographic Information Systems</i>, GIS '10, pages 99-108, New York, NY, USA,2010. ACM.
  </p>
</div>

