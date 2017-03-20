package weblog;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import scala.Tuple2;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Gautam on 2017-03-16.
 */
public class App
{
    final static int BUFFER = 2048;

    final static Minutes sessionWindowMinutes = Minutes.minutes(15);

    public static void main(String[] args) throws Exception
    {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        String fileName = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz";
        InputStream is = classloader.getResourceAsStream(fileName);
        ByteArrayOutputStream outputStream = decompressGzipFile(is);
        String[] lines = outputStream.toString().split("\\n");
        is.close();
        outputStream.close();
        List<LogLine> logLines = new ArrayList<LogLine>();
        for(String line : lines)
        {
            List<String> parts = parseLine(line);
            LogLine logLine = new LogLine();
            DateTime dt = new DateTime(parts.get(0));
            logLine.setDateTime(dt);
            logLine.setUrl(parts.get(11));
            logLine.setIpAddress(parts.get(2));
            logLines.add(logLine);
        }
        SparkConf conf = new SparkConf();
        conf.setAppName("WebLog");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<LogLine> logLinesRdd = sc.parallelize(logLines);
        JavaPairRDD<String, Map<Integer, List<LogLine>>> groupedRdd = sessionize(logLinesRdd, sessionWindowMinutes);
        groupedRdd.persist(StorageLevel.MEMORY_AND_DISK());
        Map<String, Double> averagePerUser = computeAverageSessionLengthPerUser(groupedRdd);
        Map<String, Minutes> maxPerUser = computeMaxSessionLengthPerUser(groupedRdd);
        Map<String, Map<Integer, Integer>> uniqueUrls =  computeUniqueUrlPerUserPerSession(groupedRdd);
        groupedRdd.unpersist();

    }

    private static Map<String, Double> computeAverageSessionLengthPerUser(JavaPairRDD<String, Map<Integer, List<LogLine>>> input)
    {
        return input.mapToPair(new PairFunction<Tuple2<String,Map<Integer,List<LogLine>>>, String, Double>() {
            public Tuple2<String, Double> call(Tuple2<String, Map<Integer, List<LogLine>>> stringMapTuple2) throws Exception {
                Minutes totalSessionLength = Minutes.minutes(0);
                Map<Integer, List<LogLine>> map = stringMapTuple2._2();
                for(Map.Entry<Integer, List<LogLine>> entry : map.entrySet())
                {
                    Minutes sessionLength = getSessionLength(entry.getValue());
                    totalSessionLength.plus(sessionLength);
                }
                double avg = totalSessionLength.getMinutes()/map.size();
                return Tuple2.apply(stringMapTuple2._1(), avg);
            }
        }).collectAsMap();
    }

    private static Map<String, Minutes> computeMaxSessionLengthPerUser(JavaPairRDD<String, Map<Integer, List<LogLine>>> input)
    {
        return input.mapToPair(new PairFunction<Tuple2<String,Map<Integer,List<LogLine>>>, String, Minutes>() {
            public Tuple2<String, Minutes> call(Tuple2<String, Map<Integer, List<LogLine>>> stringMapTuple2) throws Exception {
                Minutes maxSessionLength = Minutes.minutes(0);
                Map<Integer, List<LogLine>> map = stringMapTuple2._2();
                for(Map.Entry<Integer, List<LogLine>> entry : map.entrySet())
                {
                    Minutes sessionLength = getSessionLength(entry.getValue());
                    if(sessionLength.isGreaterThan(maxSessionLength))
                    {
                        maxSessionLength = sessionLength;
                    }
                }
                return Tuple2.apply(stringMapTuple2._1(), maxSessionLength);
            }
        }).collectAsMap();
    }

    private static Map<String, Map<Integer, Integer>> computeUniqueUrlPerUserPerSession(JavaPairRDD<String, Map<Integer, List<LogLine>>> input)
    {
        return input.mapToPair(new PairFunction<Tuple2<String,Map<Integer,List<LogLine>>>, String, Map<Integer, Integer>>() {
            public Tuple2<String, Map<Integer, Integer>> call(Tuple2<String, Map<Integer, List<LogLine>>> stringMapTuple2) throws Exception {
                Minutes maxSessionLength = Minutes.minutes(0);
                Map<Integer, List<LogLine>> map = stringMapTuple2._2();
                Map<Integer, Integer> out = new HashMap<Integer, Integer>();
                for(Map.Entry<Integer, List<LogLine>> entry : map.entrySet())
                {
                    List<LogLine> logs = entry.getValue();
                    int numUnique = getUniqueUrls(logs);
                    out.put(entry.getKey(), numUnique);
                }
                return Tuple2.apply(stringMapTuple2._1(), out);
            }
        }).collectAsMap();
    }

    private static int getUniqueUrls(List<LogLine> list)
    {
        int count = 0;
        Set<String> urls = new HashSet<String>();
        for(LogLine line : list)
        {
            if(!urls.contains(line.getUrl()))
            {
                urls.add(line.getUrl());
                count++;
            }
        }
        return count;
    }

    private static Minutes getSessionLength(List<LogLine> list)
    {
        DateTime minTime = null;
        DateTime maxTime = null;
        if(list.size()==0)
        {
            return Minutes.minutes(0);
        }
        for(LogLine logLine : list)
        {
            if(minTime == null)
            {
                minTime = logLine.getDateTime();
            }
            else
            {
                if(logLine.getDateTime().isBefore(minTime))
                {
                    minTime = logLine.getDateTime();
                }
            }
            if(maxTime == null)
            {
                maxTime = logLine.getDateTime();
            }
            else
            {
                if(logLine.getDateTime().isAfter(maxTime))
                {
                    maxTime = logLine.getDateTime();
                }
            }
        }
        return Minutes.minutesBetween(minTime, maxTime);
        //which way should this go
    }
    private static JavaPairRDD<String, Map<Integer, List<LogLine>>> sessionize(JavaRDD<LogLine> logLines, final Minutes sessionSize)
    {
        return logLines.sortBy(new Function<LogLine, DateTime>() {

            public DateTime call(LogLine logLine) throws Exception {
                return logLine.getDateTime();
            }
        }, true, 100)
                .groupBy(new Function<LogLine, String>() {
                    public String call(LogLine logLine) throws Exception {
                        return logLine.getIpAddress();
                    }
                }).mapValues(new Function<Iterable<LogLine>, Map<Integer, List<LogLine>>>() {
                     public Map<Integer, List<LogLine>> call(Iterable<LogLine> logLines) throws Exception {
                        Map<Integer, List<LogLine>> map = new HashMap<Integer, List<LogLine>>();
                        DateTime prevDateTime = null;
                        int counter = 0;
                        for(LogLine logLine : logLines)
                        {
                            if(prevDateTime==null)
                            {
                                List<LogLine> temp = new ArrayList<LogLine>();
                                temp.add(logLine);
                                prevDateTime = logLine.getDateTime();
                                map.put(counter, temp);
                            }
                            else
                            {
                                DateTime curDateTime = logLine.getDateTime();
                                Minutes diff = Minutes.minutesBetween(curDateTime, prevDateTime);
                                if(diff.isGreaterThan(sessionSize))
                                {
                                    counter++;
                                    List<LogLine> temp = new ArrayList<LogLine>();
                                    temp.add(logLine);
                                    map.put(counter, temp);
                                    prevDateTime = curDateTime;
                                }
                                else
                                {
                                    map.get(counter).add(logLine);
                                    prevDateTime = curDateTime;
                                }
                            }
                        }
                        return map;
                    }
                });


    }

    private static ByteArrayOutputStream decompressGzipFile(InputStream is) throws IOException
    {
        GzipCompressorInputStream  gzipIn =   new GzipCompressorInputStream(is);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[BUFFER];
        int count;
        while ((count = gzipIn.read(buffer, 0, BUFFER)) != -1) {
            outputStream.write(buffer, 0, count);
        }
        gzipIn.close();
        return outputStream;
    }

    private static List<String> parseLine(String line)
    {
        List<String> list = new ArrayList<String>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
        while (m.find())
            list.add(m.group(1).replace("\"", ""));
        return list;
    }
}