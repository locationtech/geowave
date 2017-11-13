class geowave::restservices {
  $set_public_dns = $geowave::set_public_dns
  $public_dns = $geowave::public_dns

  $line_string = "
        <context-param>
          <param-name>host_port</param-name>
          <param-value>$public_dns</param-value>
        </context-param>"
  
  if !defined(Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"]) {
    package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-restservices":
    ensure => latest,
    tag    => 'geowave-package',
    notify => Exec['wait_for_restservices_to_unpack'], #force restart of service
  }

  #This is done instead of a notify => Service['gwtomcat'] to force immediate
  #restart of the tomcat8 server. This is to ensure the war file is unpacked
  #so we can run the file_line block if needed.  
  exec { 'wait_for_restservices_to_unpack':
    require => Package ["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-restservices"],
    command => "/sbin/service gwtomcat restart && sleep 10",
  }

  if $set_public_dns{
    file_line {'set_public_dns':
      ensure  => present,
      line    => $line_string,
      path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/webapps/restservices/WEB-INF/web.xml",
      match   => "<param-value>$public_dns<\/param-value>",
      after   => "<\/context-param>",
      replace => false,
      require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-restservices"],
      notify  => Service['gwtomcat'],
    }
  }
}
