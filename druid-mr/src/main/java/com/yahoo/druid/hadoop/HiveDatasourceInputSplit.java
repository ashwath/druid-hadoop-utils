package com.yahoo.druid.hadoop;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.WindowedDataSegment;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveDatasourceInputSplit extends FileSplit implements InputSplit, JobConfigurable
{
  private static final String[] EMPTY_STR_ARRAY = new String[0];
  private static final Logger logger = LoggerFactory.getLogger(InputSplit.class);
  private List<WindowedDataSegment> segments = null;
  private String[] locations = null;
  private Path path;

  // required for deserialization
  public HiveDatasourceInputSplit()
  {
	  super((Path) null, 0, 0, (String[]) null);
	  logger.info("In first constructor");

  }

  public HiveDatasourceInputSplit(@NotNull List<WindowedDataSegment> segments, String[] locations, Path dummyPath)
  {
	super(dummyPath, 0, 0, (String[]) null);
	logger.info("In second constructor");
    Preconditions.checkArgument(segments != null && segments.size() > 0, "no segments");
    this.segments = segments;
    this.locations = locations == null ? EMPTY_STR_ARRAY : locations;
	path =  dummyPath;

  }

  @Override
  public long getLength()
  {
    long size = 0;
    for (WindowedDataSegment segment : segments) {
      size += segment.getSegment().getSize();
    }
    return size;
  }

  @Override
  public String[] getLocations()
  {
    return locations;
  }

  public List<WindowedDataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(segments));
    out.writeInt(locations.length);
    for (String location : locations) {
      out.writeUTF(location);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    segments = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        in.readUTF(),
        new TypeReference<List<WindowedDataSegment>>()
        {
        });
    locations = new String[in.readInt()];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = in.readUTF();
    }
  }

 @Override
 public Path getPath() {
	 logger.info("getPath() "+ path.toUri().getPath());
	 return  path;
 }


	@Override
	public String toString()
	{
		return "HiveDatasourceInputSplit{" +
				"segments=" + segments +
				", locations=" + Arrays.toString(locations) +
				'}';
	}

	@Override
	public void configure(JobConf conf)
	{
		logger.info("configure: "+ conf.get("druid.hive.dummyfilename"));
		path = new Path(conf.get("druid.hive.dummyfilename"));
	}
}
