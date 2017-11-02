class geowave::tomcat_server {

  $http_port = $geowave::http_port
  package { "geowave-${geowave::geowave_version}-tomcat":

    ensure => latest,
    tag    => 'geowave-package',
    notify => Service['gw_tomcat8']
  }

  if !defined(Package["geowave-${geowave::geowave_version}-core"]) {
    package { "geowave-${geowave::geowave_version}-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }
  
  file_line {'Change_default_port':
    ensure  => present,
    path    => '/usr/local/geowave/tomcat8/conf/server.xml',
    line    => "<Connector port=\"${http_port}\" protocol=\"HTTP/1.1\"",
    match   => '.Connector\ port="(\d{1,5}".protocol="HTTP.*"$)',
  }
}
