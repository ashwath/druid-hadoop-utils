package com.yahoo.druid.hadoop;

import io.druid.data.input.MapBasedInputRow;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;

public class DruidInputRow implements Writable
{
  Map<String, Object> obj;
  public DruidInputRow()
  {
    obj = new HashMap<>();
  }
	public DruidInputRow(DateTime timestamp, List<String> dimensions, Map<String, Object> event)
	{
    obj = new MapBasedInputRow(timestamp, dimensions, event).getEvent();
	}

  public Map<String, Object> getEvent()
  {
    return obj;
  }

	@Override
	public void write(DataOutput out) throws IOException
	{
    throw new UnsupportedOperationException();

	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
    throw new UnsupportedOperationException();

	}
}
