%define timestamp   %(date +%Y%m%d%H%M)
%define name        %{?_name}%{!?_name: geowave}
%define version     %{?_version}%{!?_version: UNKNOWN}
%define buildroot   %{_topdir}/BUILDROOT/%{name}-%{version}-root
%define __jar_repack %{nil}

%define geowave_home /usr/local/geowave
%define geowave_accumulo_home %{geowave_home}/accumulo
%define geowave_docs_home %{geowave_home}/docs
%define geowave_geoserver_home %{geowave_home}/geoserver
%define geowave_tools_home %{geowave_home}/tools
%define geowave_geoserver_libs %{geowave_geoserver_home}/webapps/geoserver/WEB-INF/lib
%define geowave_geoserver_data %{geowave_geoserver_home}/data_dir

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Name:           %{name}
Version:        %{version}
Release:        %{timestamp}
BuildRoot:      %{buildroot}
BuildArch:      noarch
Summary:        GeoWave provides geospatial and temporal indexing on top of Accumulo
License:        Apache2
Group:          Applications/Internet
Source0:        geowave-accumulo.jar
Source1:        deploy-geowave-to-hdfs.sh
Source2:        geoserver.zip
Source3:        geowave-geoserver.jar
Source4:        geowave-logrotate.sh
Source5:        geowave-init.sh
Source6:        bash_profile.sh
Source7:        default.xml
Source8:        namespace.xml
Source9:        workspace.xml
Source10:       geowave-tools.jar
Source11:       site.tar.gz
Source12:       puppet-scripts.tar.gz
Source13:       manpages.tar.gz
BuildRequires:  unzip
BuildRequires:  zip
BuildRequires:  xmlto
BuildRequires:  asciidoc

%description
GeoWave provides geospatial and temporal indexing on top of Accumulo.

%prep
rm -rf %{_rpmdir}/%{buildarch}/%{name}*
rm -rf %{_srcrpmdir}/%{name}*

%build
rm -fr %{_builddir}
mkdir -p %{_builddir}/%{name}

%clean
rm -fr %{buildroot}
rm -fr %{_builddir}/*

%install
rm -fr %{buildroot}
mkdir -p %{buildroot}%{geowave_accumulo_home}

# Copy Accumulo library and deployment script onto local file system
cp %{SOURCE0} %{SOURCE1} %{buildroot}%{geowave_accumulo_home}

# Extract version info file for easy inspection
unzip -p %{SOURCE0} build.properties > %{buildroot}%{geowave_accumulo_home}/geowave-accumulo-build.properties

# Unpack and rename prepackaged jetty/geoserver
unzip -qq  %{SOURCE2} -d %{buildroot}%{geowave_home}
mv %{buildroot}%{geowave_geoserver_home}-* %{buildroot}%{geowave_geoserver_home}

# patch some config settings
sed -i 's/yyyy_mm_dd.//g' %{buildroot}%{geowave_geoserver_home}/etc/jetty.xml

# Remove cruft we don't want in our deployment
rm -fr %{buildroot}%{geowave_geoserver_home}/bin/*.bat
rm -fr %{buildroot}%{geowave_geoserver_home}/data_dir/layergroups/*
rm -fr %{buildroot}%{geowave_geoserver_home}/data_dir/workspaces/*
rm -fr %{buildroot}%{geowave_geoserver_home}/logs/keepme.txt

# Copy our geowave library into place
mkdir -p %{buildroot}%{geowave_geoserver_libs}
cp %{SOURCE3} %{buildroot}%{geowave_geoserver_libs}

# Copy system service files into place
mkdir -p %{buildroot}/etc/logrotate.d
cp %{SOURCE4} %{buildroot}/etc/logrotate.d/geowave
mkdir -p %{buildroot}/etc/init.d
cp %{SOURCE5} %{buildroot}/etc/init.d/geowave
mkdir -p %{buildroot}/etc/profile.d
cp %{SOURCE6} %{buildroot}/etc/profile.d/geowave.sh

# Copy over our custom workspace config files
mkdir -p %{buildroot}%{geowave_geoserver_data}/workspaces/geowave
cp %{SOURCE7} %{buildroot}%{geowave_geoserver_data}/workspaces
cp %{SOURCE8} %{buildroot}%{geowave_geoserver_data}/workspaces/geowave
cp %{SOURCE9} %{buildroot}%{geowave_geoserver_data}/workspaces/geowave

# Stage geowave tools
mkdir -p %{buildroot}%{geowave_tools_home}/plugins
cp %{SOURCE10} %{buildroot}%{geowave_tools_home}
cp %{buildroot}%{geowave_accumulo_home}/geowave-accumulo-build.properties %{buildroot}%{geowave_tools_home}/build.properties
pushd %{buildroot}%{geowave_tools_home}
zip -g %{buildroot}%{geowave_tools_home}/geowave-tools.jar build.properties
popd
mv %{buildroot}%{geowave_tools_home}/build.properties %{buildroot}%{geowave_tools_home}/geowave-tools-build.properties
unzip -p %{SOURCE10} geowave-tools.sh > %{buildroot}%{geowave_tools_home}/geowave-tools.sh
# TODO: Add bash completion back for new geowave tool
#mkdir -p %{buildroot}/etc/bash_completion.d
#unzip -p %{SOURCE10} geowave-tools-cmd-completion.sh > %{buildroot}/etc/bash_completion.d/geowave-tools-cmd-completion.sh

# Copy documentation into place
mkdir -p %{buildroot}%{geowave_docs_home}
tar -xzf %{SOURCE11} -C %{buildroot}%{geowave_docs_home} --strip=1

# Copy man pages into place
mkdir -p %{buildroot}/usr/local/share/man/man1
tar -xvf %{SOURCE13} -C %{buildroot}/usr/local/share/man/man1
rm -rf %{buildroot}%{geowave_docs_home}/manpages
rm -f %{buildroot}%{geowave_docs_home}/*.pdfmarks

# Puppet scripts
mkdir -p %{buildroot}/etc/puppet/modules
tar -xzf %{SOURCE12} -C %{buildroot}/etc/puppet/modules

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package        single-host
Summary:        All GeoWave Components
Group:          Applications/Internet
Requires:       %{name}-accumulo
Requires:       %{name}-jetty
Requires:       %{name}-tools

%description single-host
GeoWave provides geospatial and temporal indexing on top of Accumulo.
This package installs the accumulo, geoserver and tools components and
would likely be useful for dev environments

%files single-host
# This is a meta-package and only exists to install other packages

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package        accumulo
Summary:        GeoWave Accumulo Components
Group:          Applications/Internet
Provides:       %{name}-accumulo = %{version}
Requires:       %{name}-core

%description accumulo
GeoWave provides geospatial and temporal indexing on top of Accumulo.
This package installs the Accumulo components of GeoWave

%post accumulo
/bin/bash %{geowave_accumulo_home}/deploy-geowave-to-hdfs.sh >> %{geowave_accumulo_home}/geowave-to-hdfs.log 2>&1

%files accumulo
%attr(755, hdfs, hdfs) %{geowave_accumulo_home}
%attr(644, hdfs, hdfs) %{geowave_accumulo_home}/geowave-accumulo.jar
%attr(755, hdfs, hdfs) %{geowave_accumulo_home}/deploy-geowave-to-hdfs.sh

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package        core
Summary:        GeoWave Core
Group:          Applications/Internet
Provides:       %{name}-core = %{version}

%description core
GeoWave provides geospatial and temporal indexing on top of Accumulo.
This package installs the GeoWave home directory and user account

%pre core
getent group geowave > /dev/null || /usr/sbin/groupadd -r geowave
getent passwd geowave > /dev/null || /usr/sbin/useradd --system --home /usr/local/geowave -g geowave geowave -c "GeoWave Application Account"

%postun core
if [ $1 -eq 0 ]; then
  /usr/sbin/userdel geowave
fi

%files core
%defattr(644, geowave, geowave, 755)
%dir %{geowave_home}

%attr(644, root, root) /etc/profile.d/geowave.sh

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package        docs
Summary:        GeoWave Documentation
Group:          Applications/Internet
Provides:       %{name}-docs = %{version}
Requires:       %{name}-core

%description docs
GeoWave provides geospatial and temporal indexing on top of Accumulo.
This package installs the GeoWave documentation into the GeoWave directory

%files docs
%defattr(644, geowave, geowave, 755)
%doc %{geowave_docs_home}

%doc %attr(644 root, root) /usr/local/share/man/man1/geowave-ingest.1
%doc %attr(644 root, root) /usr/local/share/man/man1/geowave-ingest-clear.1
%doc %attr(644 root, root) /usr/local/share/man/man1/geowave-ingest-hdfsingest.1
%doc %attr(644 root, root) /usr/local/share/man/man1/geowave-ingest-hdfsstage.1
%doc %attr(644 root, root) /usr/local/share/man/man1/geowave-ingest-localingest.1
%doc %attr(644 root, root) /usr/local/share/man/man1/geowave-ingest-poststage.1

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package        jetty
Summary:        GeoWave GeoServer Components
Group:          Applications/Internet
Provides:       %{name}-jetty = %{version}
Requires:       %{name}-core

%description jetty
GeoWave provides geospatial and temporal indexing on top of Accumulo.
This package installs the Accumulo components of GeoWave

%post jetty
/sbin/chkconfig --add geowave
chown -R geowave:geowave /usr/local/geowave
exit 0

%preun jetty
/sbin/service geowave stop >/dev/null 2>&1
exit 0

%files jetty
%defattr(644, geowave, geowave, 755) 
%{geowave_geoserver_home}

%attr(755, geowave, geowave) %{geowave_geoserver_home}/bin

%config %defattr(644, geowave, geowave, 755)
%{geowave_geoserver_home}/etc

%config %defattr(644, geowave, geowave, 755)
%{geowave_geoserver_data}

%attr(644, root, root) /etc/logrotate.d/geowave
%attr(755, root, root) /etc/init.d/geowave

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package        tools
Summary:        GeoWave Tools
Group:          Applications/Internet
Provides:       %{name}-tools = %{version}
Requires:       %{name}-core

%description tools
GeoWave provides geospatial and temporal indexing on top of Accumulo.
This package installs GeoWave tools utility

%post tools
ln -fs /usr/local/geowave/tools/geowave-tools.sh /usr/local/bin/geowave

%postun tools
if [ $1 -eq 0 ]; then
  rm -f /usr/local/bin/geowave
fi

%files tools
%defattr(644, geowave, geowave, 755)
%{geowave_tools_home}

%attr(755, geowave, geowave) %{geowave_tools_home}/geowave-tools.sh
# %attr(644, root, root) /etc/bash_completion.d/geowave-tools-cmd-completion.sh

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package        puppet
Summary:        GeoWave Puppet Scripts
Group:          Applications/Internet
Requires:       puppet-server

%description puppet
This package installs the geowave Puppet module to /etc/puppet/modules

%files puppet
%defattr(644, root, root, 755)
/etc/puppet/modules/geowave

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%changelog
* Thu May 7 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.7
- Use alternatives to support parallel version installs
- Replace geowave-ingest with geowave-tools
* Thu Jan 15 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.2-3
- Added man pages
* Mon Jan 5 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.2-2
- Added geowave-puppet rpm
* Fri Jan 2 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.2-1
- Added a helper script for geowave-ingest and bash command completion
* Wed Nov 19 2014 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.2
- First packaging
