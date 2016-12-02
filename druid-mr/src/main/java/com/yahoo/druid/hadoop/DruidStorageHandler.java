//package com.yahoo.druid.hadoop;
//
//
//import org.apache.commons.lang3.StringUtils;
//import org.apache.hadoop.hive.metastore.HiveMetaHook;
//import org.apache.hadoop.hive.metastore.MetaStoreUtils;
//import org.apache.hadoop.hive.metastore.api.MetaException;
//import org.apache.hadoop.hive.metastore.api.Table;
//import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
//import org.apache.hadoop.hive.serde2.SerDe;
//import org.apache.hadoop.mapred.InputFormat;
//import org.apache.hadoop.mapred.OutputFormat;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
// */
//@SuppressWarnings({ "deprecation", "rawtypes" })
//public class DruidStorageHandler extends DefaultStorageHandler implements HiveMetaHook
//{
//
//  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);
//
//  @Override
//  public Class<? extends InputFormat> getInputFormatClass()
//  {
//    return HiveDatasourceInputFormat.class;
//  }
//
//  @Override
//  public Class<? extends OutputFormat> getOutputFormatClass()
//  {
//    return org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat.class;
//  }
//
//  @Override
//  public Class<? extends SerDe> getSerDeClass()
//  {
//    return DruidSerde.class;
//  }
//
//  @Override
//  public HiveMetaHook getMetaHook()
//  {
//    return this;
//  }
//
//  @Override
//  public void preCreateTable(Table table) throws MetaException
//  {
//    // Do safety checks
//    if (!MetaStoreUtils.isExternalTable(table)) {
//      throw new MetaException("Table in Druid needs to be declared as EXTERNAL");
//    }
//    if (!StringUtils.isEmpty(table.getSd().getLocation())) {
//      throw new MetaException("LOCATION may not be specified for Druid");
//    }
//    if (table.getPartitionKeysSize() != 0) {
//      throw new MetaException("PARTITIONED BY may not be specified for Druid");
//    }
//    if (table.getSd().getBucketColsSize() != 0) {
//      throw new MetaException("CLUSTERED BY may not be specified for Druid");
//    }
//  }
//
//  @Override
//  public void rollbackCreateTable(Table table) throws MetaException
//  {
//    // Nothing to do
//  }
//
//  @Override
//  public void commitCreateTable(Table table) throws MetaException
//  {
//    // Nothing to do
//  }
//
//  @Override
//  public void preDropTable(Table table) throws MetaException
//  {
//    // Nothing to do
//  }
//
//  @Override
//  public void rollbackDropTable(Table table) throws MetaException
//  {
//    // Nothing to do
//  }
//
//  @Override
//  public void commitDropTable(Table table, boolean deleteData) throws MetaException
//  {
//    // Nothing to do
//  }
//
//  @Override
//  public String toString()
//  {
//    return "com.yahoo.druid.hadoop.DruidStorageHandler";
//  }
//
//}
