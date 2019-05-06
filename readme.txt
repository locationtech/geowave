mvn package -DskipTests
mvn package -e

关于google.protobuf定义的位置

定义的protobuf的文件的位置
geowave-parent/extensions/datastores/hbase/src/main/protobuf
自动生成的java类的位置
(Maven Projects面板中GeoWave Hbase/Lifecycle/clean,compile,package可生成对应的java类)
geowave-parent/extensions/datastores/hbase/src/main/java/org.locationtech.geowave.datastore.hbase/coprocessors/protobuf/*.java

geowave官方源码打包命令
mvn package -P hbase-container-singlejar -DskipTests

geoserver 数据源插件的代码位置
org.locationtech.geowave.adapter.vector.plugin.GeoWaveGTDataStoreFactory
extensions/adapters/vector/src/main/resources/META-INFO.services
extensions/adapters/vector/src/main/java/org/locationtech.geowave.adapter.vector/plugin/GeoWaveGTDataStoreFactory

1

