class geowave::server {

  $http_port = $geowave::http_port

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-jetty":
    ensure => latest,
    tag    => 'geowave-package',
    notify => Service['geowave']
  }

  if !defined(Package["geowave-core"]) {
    package { "geowave-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  file {'/usr/local/geowave/geoserver/etc/jetty.xml':
    ensure  => file,
    content => template('geowave/jetty.xml.erb'),
    owner   => 'geowave',
    group   => 'geowave',
    mode    => '644',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-jetty"],
    notify  => Service['geowave']
  }

}
