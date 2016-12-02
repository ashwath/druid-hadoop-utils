package com.yahoo.druid.hadoop;

import io.druid.data.input.InputRow;
import io.druid.indexer.hadoop.DatasourceRecordReader;
import io.druid.indexer.hadoop.SegmentInputRow;
import java.io.IOException;
import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DruidRecordReader extends DatasourceRecordReader
{
	private static final Logger logger = LoggerFactory.getLogger(DruidRecordReader.class);
	public DruidRecordReader()
	{
		super();
	}

	@Override
	public InputRow getCurrentValue() throws IOException, InterruptedException
	{
    return null;
    // DruidInputRow druidInputRow = null;
    // SegmentInputRow segmentInputRow = (SegmentInputRow)
    // super.getCurrentValue();
    // Field field = null;
    // try
    // {
    // field = segmentInputRow.getClass().getDeclaredField("delegate");
    // field.setAccessible(true);
    // druidInputRow = (DruidInputRow) field.get(segmentInputRow);
    // } catch (Exception e)
    // {
    // logger.info(e.getMessage());
    // }
    //
    // return druidInputRow;
	}
}
