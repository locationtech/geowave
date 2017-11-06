class geowave::gwtomcat8_server {
  $http_port = $geowave::http_port

  if !defined(Package["geowave-${geowave::geowave_version}-core"]) {
    package { "geowave-${geowave::geowave_version}-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  if !defined(Package["geowave-${geowave::geowave_version}-gwtomcat8"]) {
    package { "geowave-${geowave::geowave_version}-gwtomcat8":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  file_line {'Change_default_port':
    ensure  => present,
    path    => '/usr/local/geowave/tomcat8/conf/server.xml',
    line    => "<Connector port=\"${http_port}\" protocol=\"HTTP/1.1\"",
    match   => '.Connector\ port="(\d{1,5}".protocol="HTTP.*"$)',
    notify  => Service['gwtomcat8']
  }
}
