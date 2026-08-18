USE [ingest_reference];

--#region TOPO-TCH

insert into dbo.ETLConfiguration (ConfigurationFilter, ConfiguredValue, Description) values (
'ConnStr_TOPO-TCH-G025_CDC',
'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.28.83.69)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=tchpuat)));AppB1#gdat$123',
'Conenction Config Of Connector TOPO-TCH-G025');

insert into dbo.ETLConfiguration (ConfigurationFilter, ConfiguredValue, Description) values (
'ConnStr_TOPO-TCH-G026_CDC',
'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.28.83.69)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=tchpuat)));AppB1#gdat$123',
'Conenction Config Of Connector TOPO-TCH-G026');

insert into dbo.ETLConfiguration (ConfigurationFilter, ConfiguredValue, Description) values (
'ConnStr_TOPO-TCH-G027_CDC',
'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.28.83.69)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=tchpuat)));AppB1#gdat$123',
'Conenction Config Of Connector TOPO-TCH-G027');

--#endregion

--#region TOPO-CLI

insert into dbo.ETLConfiguration (ConfigurationFilter, ConfiguredValue, Description) values (
'ConnStr_TOPO_CLI_G041_CDC',
'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=OFF)(FAILOVER=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=172.28.83.70)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=172.16.100.105)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=topouat)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))));Hdp789#Etl',
'Conenction Config Of Connector TOPO_CLI-G041');

insert into dbo.ETLConfiguration (ConfigurationFilter, ConfiguredValue, Description) values (
'ConnStr_TOPO_CLI_G042_CDC',
'${file:/opt/kafka-connect/secrets/topo-cli-credential.properties:db.url};Hdp789#Etl',
'Conenction Config Of Connector TOPO_CLI-G042');

insert into dbo.ETLConfiguration (ConfigurationFilter, ConfiguredValue, Description) values (
'ConnStr_TOPO_CLI_G043_CDC',
'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=OFF)(FAILOVER=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=172.28.83.70)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=172.16.100.105)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=topouat)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=180)(DELAY=5))));Hdp789#Etl',
'Conenction Config Of Connector TOPO_CLI-G043');

--#endregion

--#region OTM-TMS

insert into dbo.ETLConfiguration (ConfigurationFilter, ConfiguredValue, Description) values (

'ConnStr_OTM_TMS_G007_CDC',
'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.252.96)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=otmtest)));Hdp789#Etl',
'Conenction Config Of Connector OTM_TMS-G007');

--#endregion