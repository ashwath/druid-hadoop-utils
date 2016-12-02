package com.yahoo.druid.hadoop;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.query.Druids;
import io.druid.query.Druids.SegmentMetadataQueryBuilder;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SerDeSpec(schemaProps = {Constants.DRUID_DATA_SOURCE, Constants.HIVE_DRUID_BROKER})
public class DruidSerde extends AbstractSerDe
{

	protected static final Logger LOG = LoggerFactory.getLogger(DruidSerde.class);

	private String[] columns;
	private PrimitiveTypeInfo[] types;
	private ObjectInspector inspector;

	/*
	   * This method converts from the String representation of Druid type to the
	   * corresponding Hive type
	   */
	public static PrimitiveTypeInfo convertDruidToHiveType(String typeName)
	{
		typeName = typeName.toUpperCase();
		switch (typeName)
		{
			case "FLOAT":
				return TypeInfoFactory.floatTypeInfo;
			case "LONG":
				return TypeInfoFactory.longTypeInfo;
			case "STRING":
				return TypeInfoFactory.stringTypeInfo;
			case "TIMESTAMP":
				return TypeInfoFactory.timestampTypeInfo;
			case "BIGINT":
				return TypeInfoFactory.longTypeInfo;
			case "INT":
				return TypeInfoFactory.intTypeInfo;
			default:
				// This is a guard for special Druid types e.g. hyperUnique
				// (http://druid.io/docs/0.9.1.1/querying/aggregations.html#hyperunique-aggregator).
				// Currently, we do not support doing anything special with them in Hive.
				// However, those columns are there, and they can be actually read as
				// normal
				// dimensions e.g. with a select query. Thus, we print the warning and
				// just read them
				// as String.
				LOG.warn("Transformation to STRING for unknown type " + typeName);
				return TypeInfoFactory.stringTypeInfo;
		}
	}

	@Override
	public void initialize(Configuration configuration, Properties properties) throws SerDeException
	{
		final List<String> columnNames = new ArrayList<>();
		final List<PrimitiveTypeInfo> columnTypes = new ArrayList<>();
		List<ObjectInspector> inspectors = new ArrayList<>();

		String columnNameProperty, columnTypeProperty;
		columnNameProperty = configuration.get(serdeConstants.LIST_COLUMNS);
		columnTypeProperty = configuration.get(serdeConstants.LIST_COLUMN_TYPES);

		if (columnNameProperty == null || "".equals(columnNameProperty) || columnNameProperty == null || "".equals(columnTypeProperty))
		{
			// Druid query
			String druidQuery = properties.getProperty(Constants.DRUID_QUERY_JSON);
			if (druidQuery == null)
			{
				// No query. We need to create a Druid Segment Metadata query that
				// retrieves all
				// columns present in the data source (dimensions and metrics).
				// Create Segment Metadata Query
				String dataSource = properties.getProperty(Constants.DRUID_DATA_SOURCE);
				if (dataSource == null)
				{
					throw new SerDeException("Druid data source not specified; use " +
							Constants.DRUID_DATA_SOURCE + " in table properties");
				}
				SegmentMetadataQueryBuilder builder = new Druids.SegmentMetadataQueryBuilder();
				builder.dataSource(dataSource);
				builder.merge(true);
				builder.analysisTypes();
				SegmentMetadataQuery query = builder.build();

				// Execute query in Druid
				String address = properties.getProperty(Constants.HIVE_DRUID_BROKER);
				if (address == null || address.isEmpty())
				{
					throw new SerDeException("Druid broker address not specified in configuration");
				}

				// Infer schema
				SegmentAnalysis schemaInfo;
				try
				{
					schemaInfo = submitMetadataRequest(address, query);
				} catch (IOException e)
				{
					throw new SerDeException(e);
				}
				for (Entry<String, ColumnAnalysis> columnInfo : schemaInfo.getColumns().entrySet())
				{
					if (columnInfo.getKey().equals("__time"))
					{
						// Special handling for timestamp column
						columnNames.add(columnInfo.getKey()); // field name
						PrimitiveTypeInfo type = TypeInfoFactory.timestampTypeInfo; // field
						// type
						columnTypes.add(type);
						inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
						continue;
					}
					columnNames.add(columnInfo.getKey()); // field name
					PrimitiveTypeInfo type = convertDruidToHiveType(
							columnInfo.getValue().getType()); // field type
					columnTypes.add(type);
					inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
				}
				columns = columnNames.toArray(new String[columnNames.size()]);
				types = columnTypes.toArray(new PrimitiveTypeInfo[columnTypes.size()]);
				inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
			} else
			{
				// Query is specified, we can extract the results schema from the query
				Query<?> query;
				try
				{
					query = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(druidQuery, Query.class);
				} catch (Exception e)
				{
					throw new SerDeException(e);
				}

				switch (query.getType())
				{
					case Query.TIMESERIES:
						inferSchema((TimeseriesQuery) query, columnNames, columnTypes);
						break;
					case Query.TOPN:
						inferSchema((TopNQuery) query, columnNames, columnTypes);
						break;
					case Query.SELECT:
						inferSchema((SelectQuery) query, columnNames, columnTypes);
						break;
					case Query.GROUP_BY:
						inferSchema((GroupByQuery) query, columnNames, columnTypes);
						break;
					default:
						throw new SerDeException("Not supported Druid query");
				}

				columns = new String[columnNames.size()];
				types = new PrimitiveTypeInfo[columnNames.size()];
				for (int i = 0; i < columnTypes.size(); ++i)
				{
					columns[i] = columnNames.get(i);
					types[i] = columnTypes.get(i);
					inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(types[i]));
				}
				inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
			}
			StringBuffer columnNameStringBuffer = new StringBuffer();
			for (int i = 0; i < columns.length - 1; i++)
			{
				columnNameStringBuffer.append(columns[i]);
				columnNameStringBuffer.append(",");
			}
			columnNameStringBuffer.append(columns[columns.length - 1]);

			StringBuffer columnTypeStringbuffer = new StringBuffer();
			for (int i = 0; i < types.length - 1; i++)
			{
				columnTypeStringbuffer.append(types[i]);
				columnTypeStringbuffer.append(",");
			}
			columnTypeStringbuffer.append(types[types.length - 1]);
			configuration.set(serdeConstants.LIST_COLUMNS, columnNameStringBuffer.toString());
			configuration.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeStringbuffer.toString());
			configuration.set("test.param", "testParam");
		} else
		{
			LOG.info("columnNameProperty " + columnNameProperty + " columnTypeProperty " + columnTypeProperty);
			// all table column names
			if (columnNameProperty.length() != 0)
			{
				columns = columnNameProperty.split(",");
				columnNames.addAll(Arrays.asList(columns));
			}
			// all column types
			if (columnTypeProperty.length() != 0)
			{
				for (String each : columnTypeProperty.split(","))
				{
					PrimitiveTypeInfo type = convertDruidToHiveType(each);
					columnTypes.add(type);
					inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
				}
			}
			types = columnTypes.toArray(new PrimitiveTypeInfo[columnTypes.size()]);
			inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
		}

		if (LOG.isDebugEnabled())
		{
			LOG.debug("DruidSerDe initialized with\n"
					+ "\t columns: " + columnNames
					+ "\n\t types: " + columnTypes);
		}
	}

	/* Submits the request and returns */
	protected SegmentAnalysis submitMetadataRequest(String address, SegmentMetadataQuery query)
			throws SerDeException, IOException
	{
		HttpClient client = HttpClientInit.createClient(HttpClientConfig.builder().build(), new Lifecycle());
		InputStream response;
		try
		{
			response = DruidStorageHandlerUtils.submitRequest(client,
					DruidStorageHandlerUtils.createRequest(address, query));
		} catch (Exception e)
		{
			throw new SerDeException(StringUtils.stringifyException(e));
		}

		// Retrieve results
		List<SegmentAnalysis> resultsList;
		try
		{
			resultsList = DruidStorageHandlerUtils.SMILE_MAPPER.readValue(response,
					new TypeReference<List<SegmentAnalysis>>()
					{
					});

		} catch (Exception e)
		{
			response.close();
			throw new SerDeException(StringUtils.stringifyException(e));
		}
		if (resultsList == null || resultsList.isEmpty())
		{
			throw new SerDeException("Connected to Druid but could not retrieve datasource information");
		}
		if (resultsList.size() != 1)
		{
			throw new SerDeException("Information about segments should have been merged");
		}

		return resultsList.get(0);
	}

	/* Timeseries query */
	private void inferSchema(TimeseriesQuery query, List<String> columnNames,
							 List<PrimitiveTypeInfo> columnTypes)
	{
		// Timestamp column
		columnNames.add("__time");
		columnTypes.add(TypeInfoFactory.timestampTypeInfo);
		// Aggregator columns
		for (AggregatorFactory af : query.getAggregatorSpecs())
		{
			columnNames.add(af.getName());
			columnTypes.add(convertDruidToHiveType(af.getTypeName()));
		}
		// Post-aggregator columns
		for (PostAggregator pa : query.getPostAggregatorSpecs())
		{
			columnNames.add(pa.getName());
			columnTypes.add(TypeInfoFactory.floatTypeInfo);
		}
	}

	/* TopN query */
	private void inferSchema(TopNQuery query, List<String> columnNames, List<PrimitiveTypeInfo> columnTypes)
	{
		// Timestamp column
		columnNames.add("__time");
		columnTypes.add(TypeInfoFactory.timestampTypeInfo);
		// Dimension column
		columnNames.add(query.getDimensionSpec().getOutputName());
		columnTypes.add(TypeInfoFactory.stringTypeInfo);
		// Aggregator columns
		for (AggregatorFactory af : query.getAggregatorSpecs())
		{
			columnNames.add(af.getName());
			columnTypes.add(convertDruidToHiveType(af.getTypeName()));
		}
		// Post-aggregator columns
		for (PostAggregator pa : query.getPostAggregatorSpecs())
		{
			columnNames.add(pa.getName());
			columnTypes.add(TypeInfoFactory.floatTypeInfo);
		}
	}

	/* Select query */
	private void inferSchema(SelectQuery query, List<String> columnNames,
							 List<PrimitiveTypeInfo> columnTypes)
	{
		// Timestamp column
		columnNames.add("__time");
		columnTypes.add(TypeInfoFactory.timestampTypeInfo);
		// Dimension columns
		for (DimensionSpec ds : query.getDimensions())
		{
			columnNames.add(ds.getOutputName());
			columnTypes.add(TypeInfoFactory.stringTypeInfo);
		}
		// Metric columns
		for (String metric : query.getMetrics())
		{
			columnNames.add(metric);
			columnTypes.add(TypeInfoFactory.floatTypeInfo);
		}
	}

	/* GroupBy query */
	private void inferSchema(GroupByQuery query, List<String> columnNames, List<PrimitiveTypeInfo> columnTypes)
	{
		// Timestamp column
		columnNames.add("__time");
		columnTypes.add(TypeInfoFactory.timestampTypeInfo);
		// Dimension columns
		for (DimensionSpec ds : query.getDimensions())
		{
			columnNames.add(ds.getOutputName());
			columnTypes.add(TypeInfoFactory.stringTypeInfo);
		}
		// Aggregator columns
		for (AggregatorFactory af : query.getAggregatorSpecs())
		{
			columnNames.add(af.getName());
			columnTypes.add(convertDruidToHiveType(af.getTypeName()));
		}
		// Post-aggregator columns
		for (PostAggregator pa : query.getPostAggregatorSpecs())
		{
			columnNames.add(pa.getName());
			columnTypes.add(TypeInfoFactory.floatTypeInfo);
		}
	}

	/**
	 * This method does the work of deserializing a record into Java objects that
	 * Hive can work with via the ObjectInspector interface.
	 */
	@Override
	public Object deserialize(Writable blob) throws SerDeException
	{
		LOG.info("deserializing blob...");

		DruidInputRow druidInputRow = (DruidInputRow) blob;
		List<Object> output = Lists.newArrayListWithExpectedSize(columns.length);
		for (int i = 0; i < columns.length; i++)
		{
			final Object value = druidInputRow.getEvent().get(columns[i]);
			if (value == null)
			{
				output.add(null);
				continue;
			}
			switch (types[i].getPrimitiveCategory())
			{
				case TIMESTAMP:
					output.add(new TimestampWritable(new Timestamp((Long) value)));
					break;
				case LONG:
					output.add(new LongWritable(((Number) value).longValue()));
					break;
				case FLOAT:
					output.add(new FloatWritable(((Number) value).floatValue()));
					break;
				case STRING:
					output.add(new Text(value.toString()));
					break;
				default:
					throw new SerDeException("Unknown type: " + types[i].getPrimitiveCategory());
			}
		}
		return output;
		// Map<?,?> root;
		// row.clear();
		// row.add(1l);
		// try {
		// DruidInputRow druidInputRow = (DruidInputRow) blob;
		// LOG.info("druidInputRow.getEvent()" + druidInputRow.getEvent());
		// Map<String, Object> event = druidInputRow.getEvent();
		// for(Map.Entry<String, Object> each: event.entrySet()){
		// LOG.info("event Key:" + each.getKey());
		// LOG.info("event Value:" + each.getValue());
		// }
		//
		// LOG.info("log druid segment dimensions");
		// for (String columns : druidInputRow.getDimensions())
		// {
		// LOG.info(columns);
		// }
		//
		// } catch (Exception e) {
		// throw new SerDeException(e);
		// }

//		// Lowercase the keys as expected by hive
//		Map<String, Object> lowerRoot = new HashMap();
//		for(Map.Entry entry: root.entrySet()) {
//			lowerRoot.put(((String)entry.getKey()).toLowerCase(), entry.getValue());
//		}
//		root = lowerRoot;
//
//		Object value= null;
//		for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
//			try {
//				TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
//				value = parseField(root.get(fieldName), fieldTypeInfo);
//			} catch (Exception e) {
//				value = null;
//			}
//			row.add(value);
//		}
	}

	/**
	 * Return an ObjectInspector for the row of data
	 */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException
	{
		return inspector;
	}

	@Override
	public SerDeStats getSerDeStats()
	{
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass()
	{
		return NullWritable.class;
	}

	@Override
	public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException
	{
		return NullWritable.get();
	}
}
