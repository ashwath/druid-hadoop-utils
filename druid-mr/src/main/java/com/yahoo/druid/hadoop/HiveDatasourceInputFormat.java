/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.yahoo.druid.hadoop;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import io.druid.collections.CountingMap;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.JobHelper;
import io.druid.indexer.hadoop.DatasourceIngestionSpec;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveDatasourceInputFormat implements InputFormat<NullWritable, DruidInputRow>
{
  private static final Logger logger = LoggerFactory.getLogger(HiveDatasourceInputFormat.class);

  public static final String CONF_DRUID_OVERLORD_HOSTPORT = "druid.overlord.hostport";
  public static final String CONF_INPUT_SEGMENTS = "druid.segments";
  public static final String CONF_DRUID_SCHEMA = "druid.datasource.schema";
  public static final String CONF_MAX_SPLIT_SIZE = "druid.datasource.split.max.size";

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException
  {
	logger.info("checkPost #5");

    String overlordUrl = jobConf.get(CONF_DRUID_OVERLORD_HOSTPORT);
    Preconditions.checkArgument(
        overlordUrl != null && !overlordUrl.isEmpty(),
        CONF_DRUID_OVERLORD_HOSTPORT + " not defined");

    logger.info("druid overlord url = " + overlordUrl);

    String schemaStr = jobConf.get(CONF_DRUID_SCHEMA);

    Preconditions.checkArgument(
        schemaStr != null && !schemaStr.isEmpty(),
        "schema undefined,  provide " + CONF_DRUID_SCHEMA);
    logger.info("schema = " + schemaStr);

    DatasourceIngestionSpec ingestionSpec = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        schemaStr,
        DatasourceIngestionSpec.class);
    String segmentsStr = getSegmentsToLoad(
        ingestionSpec.getDataSource(),
        ingestionSpec.getIntervals(),
        overlordUrl);
    logger.info("segments list received from overlord = " + segmentsStr);

    List<DataSegment> segmentsList = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        segmentsStr,
        new TypeReference<List<DataSegment>>()
        {
        });
    VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    for (DataSegment segment : segmentsList) {
      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
    }
    final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = timeline
        .lookup(ingestionSpec.getIntervals().get(0));
    final List<WindowedDataSegment> windowedSegments = new ArrayList<>();
    for (TimelineObjectHolder<String, DataSegment> holder : timeLineSegments) {
      for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
        windowedSegments.add(new WindowedDataSegment(chunk.getObject(), holder.getInterval()));
      }
    }

    jobConf.set(CONF_INPUT_SEGMENTS, HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(windowedSegments));

    segmentsStr = Preconditions.checkNotNull(jobConf.get(CONF_INPUT_SEGMENTS), "No segments found to read");
    List<WindowedDataSegment> segments = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        segmentsStr,
        new TypeReference<List<WindowedDataSegment>>()
        {
        });
    if (segments == null || segments.size() == 0) {
      throw new ISE("No segments found to read");
    }

    logger.info("segments to read "+ segmentsStr);

    long maxSize = numSplits;

    if (maxSize > 0) {
      // combining is to happen, let us sort the segments list by size so that
      // they
      // are combined appropriately
      Collections.sort(
          segments,
          new Comparator<WindowedDataSegment>()
          {
            @Override
            public int compare(WindowedDataSegment s1, WindowedDataSegment s2)
            {
              return Long.compare(s1.getSegment().getSize(), s2.getSegment().getSize());
            }
          });
    }

    List<InputSplit> splits = Lists.newArrayList();

    List<WindowedDataSegment> list = new ArrayList<>();
    long size = 0;

    // JobConf dummyConf = new JobConf();
	  Job job = new Job(jobConf);
	  JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
	  Path [] paths = org.apache.hadoop.mapreduce.lib.input.FileInputFormat.getInputPaths(jobContext);
	  logger.info("dummyPath : " + paths);

	  jobConf.set("druid.hive.dummyfilename", paths[0].toString());

    InputFormat fio = supplier.get();
    for (WindowedDataSegment segment : segments) {
      if (size + segment.getSegment().getSize() > maxSize && size > 0) {
        splits.add(toDataSourceSplit(list, fio, jobConf, paths[0]));
        list = Lists.newArrayList();
        size = 0;
      }

      list.add(segment);
      size += segment.getSegment().getSize();
    }

    if (list.size() > 0) {
      splits.add(toDataSourceSplit(list, fio, jobConf, paths[0]));
    }

    logger.info("Number of splits: " + splits.size());
	for(InputSplit split: splits){
		logger.info(split.getClass().getName());
		for(String location:  split.getLocations()) logger.info(location);
	}
    return Iterables.toArray(splits, InputSplit.class);
  }

  private Supplier<InputFormat> supplier = new Supplier<InputFormat>()
  {
    @Override
    public InputFormat get()
    {
      return new TextInputFormat()
      {
        // Always consider non-splittable as we only want to get location of
        // blocks for the segment
        // and not consider the splitting.
        // also without this, isSplitable(..) fails with NPE because
        // compressionCodecs is not properly setup.
        @Override
        protected boolean isSplitable(FileSystem fs, Path file)
        {
          return false;
        }
      };
    }
  };

  @Override
  public RecordReader<NullWritable, DruidInputRow> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter)
      throws IOException
  {
      logger.info("CheckPost6" + split.getClass());
	  return new HiveDatasourceRecordReader(split, jobConf, reporter);
  }

  private String getSegmentsToLoad(String dataSource, List<Interval> intervals, String overlordUrl)
      throws MalformedURLException
  {
    logger.info("CheckPost7");
	  String urlStr = "http://" + overlordUrl + "/druid/indexer/v1/action";
    logger.info("Sending request to overlord at " + urlStr);
    
    Interval interval = intervals.get(0);

    String requestJson = getSegmentListUsedActionJson(interval.toString());
    logger.info("request json is " + requestJson);

    int numTries = 3;
    for (int trial = 0; trial < numTries; trial++) {
		try
		{
			logger.info("attempt number {} to get list of segments from overlord", trial);
			Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("httpproxy-prod.blue.ygrid.yahoo.com", 4080));
			URL url = new URL(
					String.format("%s/druid/coordinator/v1/metadata/datasources/%s/segments?full", overlordUrl, dataSource));
			//new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection(proxy);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("content-type", "application/json");
			conn.setRequestProperty("Accept", "*/*");
			conn.setUseCaches(false);
			conn.setDoOutput(true);
			conn.setConnectTimeout(60000);
			conn.usingProxy();
			OutputStream out = conn.getOutputStream();
			out.write(requestJson.getBytes());
			out.close();
			int responseCode = conn.getResponseCode();
			if (responseCode == 200)
			{
				return IOUtils.toString(conn.getInputStream());
			} else
			{
				logger.warn(
						"Attempt Failed to get list of segments from overlord. response code [%s] , response [%s]",
						responseCode, IOUtils.toString(conn.getInputStream())
				);
			}
		} catch (Exception ex)
		{
			logger.warn("Exception in getting list of segments from overlord", ex);
		}

      try {
        Thread.sleep(5000); //wait before next trial
      }
      catch (InterruptedException ex) {
        Throwables.propagate(ex);
      }
    }
    

    throw new RuntimeException(
        String.format(
            "failed to find list of segments, dataSource[%s], interval[%s], overlord[%s]",
            dataSource,
            interval,
            overlordUrl));
    }

  protected String getSegmentListUsedActionJson(String interval)
  {
    return "[\"" + interval + "\"]";
  }

  private HiveDatasourceInputSplit toDataSourceSplit(
      List<WindowedDataSegment> segments,
      InputFormat fio,
      JobConf conf,
	  Path dummyPath)
  {
	  logger.info("CheckPost8");
    String[] locations = null;
    try {
      locations = getFrequentLocations(segments, fio, conf);
    } catch (Exception e) {
      Throwables.propagate(e);
      //logger.error("Exception thrown finding location of splits");
    }
	 Path [] paths = FileInputFormat.getInputPaths(conf);
	 logger.info("dummyPath :"+ dummyPath);

    if (locations != null) {
      logger.info("locations: ");
      for (String location : locations) {
        logger.info("location: " + location);
      }
    }
    return new HiveDatasourceInputSplit(segments, locations, dummyPath);
  }

  private String[] getFrequentLocations(
      List<WindowedDataSegment> segments,
      InputFormat fio,
      JobConf conf) throws IOException
  {
    Iterable<String> locations = Collections.emptyList();
    for (WindowedDataSegment segment : segments) {
      FileInputFormat.setInputPaths(conf, new Path(JobHelper.getURIFromSegment(segment.getSegment())));
        logger.info("CheckPost 4" + fio.getClass());
		for (InputSplit split : fio.getSplits(conf, 1)) {
        locations = Iterables.concat(locations, Arrays.asList(split.getLocations()));
      }
    }
    return getFrequentLocations(locations);
  }

  private static String[] getFrequentLocations(Iterable<String> hosts)
  {

    final CountingMap<String> counter = new CountingMap<>();
    for (String location : hosts) {
      counter.add(location, 1);
    }

    final TreeSet<Pair<Long, String>> sorted = Sets.<Pair<Long, String>> newTreeSet(
        new Comparator<Pair<Long, String>>()
        {
          @Override
          public int compare(Pair<Long, String> o1, Pair<Long, String> o2)
          {
            int compare = o2.lhs.compareTo(o1.lhs); // descending
            if (compare == 0) {
              compare = o1.rhs.compareTo(o2.rhs); // ascending
            }
            return compare;
          }
        });

    for (Map.Entry<String, AtomicLong> entry : counter.entrySet()) {
      sorted.add(Pair.of(entry.getValue().get(), entry.getKey()));
    }

    // use default replication factor, if possible
    final List<String> locations = Lists.newArrayListWithCapacity(3);
    for (Pair<Long, String> frequent : Iterables.limit(sorted, 3)) {
      locations.add(frequent.rhs);
    }
    return locations.toArray(new String[locations.size()]);
  }
}
