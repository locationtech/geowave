class geowave(
  $geowave_version        = $geowave::params::geowave_version,
  $hadoop_vendor_version  = $geowave::params::hadoop_vendor_version,
  $install_accumulo       = $geowave::params::install_accumulo,
  $install_app            = $geowave::params::install_app,
  $install_app_server     = $geowave::params::install_app_server,
  $http_port              = $geowave::params::http_port
) inherits geowave::params {

  if $geowave_version == undef { fail("geowave_version parameter is required") }
  if $hadoop_vendor_version == undef { fail("hadoop_vendor_version parameter is required") }

  if $install_accumulo {
    class {'geowave::accumulo':}
  }

  if $install_app {
    class {'geowave::app':}
  }

  if $install_app_server {
    anchor {'geowave::begin': } ->
      class {'geowave::server':} ->
      class {'geowave::service':} ->
    anchor {'geowave::end':}
  }

}
