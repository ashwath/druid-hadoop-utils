package com.yahoo.druid.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.JobHelper;
import io.druid.indexer.hadoop.DatasourceIngestionSpec;
import io.druid.indexer.hadoop.DatasourceInputFormat;
import io.druid.indexer.hadoop.DatasourceInputSplit;
import io.druid.indexer.hadoop.SegmentInputRow;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.realtime.firehose.IngestSegmentFirehose;
import io.druid.segment.realtime.firehose.WindowedStorageAdapter;

public class HiveDatasourceRecordReader implements RecordReader<NullWritable, DruidInputRow>
{
  private static final Logger logger = new Logger(HiveDatasourceRecordReader.class);

  private DatasourceIngestionSpec spec;
  private IngestSegmentFirehose firehose;

  private int rowNum;
  private MapBasedRow currRow;

  private List<QueryableIndex> indexes = Lists.newArrayList();
  private List<File> tmpSegmentDirs = Lists.newArrayList();
  private int numRows;
  private JobConf jobConf;

  public HiveDatasourceRecordReader(InputSplit split, JobConf jobConf, Reporter reporter)
  {
    this.jobConf = jobConf;
    spec = readAndVerifyDatasourceIngestionSpec(jobConf, HadoopDruidIndexerConfig.JSON_MAPPER);

    List<WindowedDataSegment> segments = ((HiveDatasourceInputSplit) split).getSegments();

    List<WindowedStorageAdapter> adapters = Lists.transform(
        segments,
        new Function<WindowedDataSegment, WindowedStorageAdapter>()
        {
          @Override
          public WindowedStorageAdapter apply(WindowedDataSegment segment)
          {
            try {
              logger.info("Getting storage path for segment [%s]", segment.getSegment().getIdentifier());
              Path path = new Path(JobHelper.getURIFromSegment(segment.getSegment()));

              logger.info("Fetch segment files from [%s]", path);

              File dir = Files.createTempDir();
              tmpSegmentDirs.add(dir);
              logger.info("Locally storing fetched segment at [%s]", dir);

              JobHelper.unzipNoGuava(path, jobConf, dir, reporter);
              logger.info("finished fetching segment files");

              QueryableIndex index = HadoopDruidIndexerConfig.INDEX_IO.loadIndex(dir);
              indexes.add(index);
              numRows += index.getNumRows();

              return new WindowedStorageAdapter(
                  new QueryableIndexStorageAdapter(index),
                  segment.getInterval());
            } catch (IOException ex) {
              throw Throwables.propagate(ex);
            }
          }
        });

    firehose = new IngestSegmentFirehose(
        adapters,
        spec.getDimensions(),
        spec.getMetrics(),
        spec.getFilter(),
        spec.getGranularity());
  }

  @Override
  public boolean next(NullWritable key, DruidInputRow value) throws IOException
  {
    logger.info("Next");
    if (firehose.hasMore()) {
      value.getEvent().clear();
      currRow = (MapBasedRow) firehose.nextRow();
      rowNum++;
      logger.info("curRow2: [%s]", currRow.toString());
      value.getEvent().putAll(new DruidInputRow(
          currRow.getTimestamp(),
          spec.getDimensions(),
          currRow.getEvent()).getEvent());
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NullWritable createKey()
  {
    return NullWritable.get();

  }

  @SuppressWarnings("unchecked")
  public DruidInputRow createValue()
  {
    return new DruidInputRow();
  }

  @Override
  public long getPos() throws IOException
  {
    return rowNum;
  }

  @Override
  public void close() throws IOException
  {
    Closeables.close(firehose, true);
    for (QueryableIndex qi : indexes) {
      Closeables.close(qi, true);
    }

    for (File dir : tmpSegmentDirs) {
      FileUtils.deleteDirectory(dir);
    }

  }

  @Override
  public float getProgress() throws IOException
  {
    if (numRows > 0) {
      return (rowNum * 1.0f) / numRows;
    } else {
      return 0;
    }
  }
  
  private DatasourceIngestionSpec readAndVerifyDatasourceIngestionSpec(Configuration config, ObjectMapper jsonMapper)
  {
    try {
      String schema = Preconditions.checkNotNull(config.get(DatasourceInputFormat.CONF_DRUID_SCHEMA), "null schema");
      logger.info("load schema [%s]", schema);

      DatasourceIngestionSpec spec = jsonMapper.readValue(schema, DatasourceIngestionSpec.class);

      if (spec.getDimensions() == null || spec.getDimensions().size() == 0) {
        throw new ISE("load schema does not have dimensions");
      }

      if (spec.getMetrics() == null || spec.getMetrics().size() == 0) {
        throw new ISE("load schema does not have metrics");
      }

      return spec;
    }
    catch (IOException ex) {
      throw new RuntimeException("couldn't load segment load spec", ex);
    }
  }

}
