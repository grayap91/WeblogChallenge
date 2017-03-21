package weblog;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.Seconds;
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

    final static Seconds sessionWindowSeconds = Seconds.seconds(15*60);

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
            logLine.setIpAddress(parts.get(2).split(":")[0]);
            logLines.add(logLine);
        }
        SparkConf conf = new SparkConf();
        conf.setAppName("WebLog");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<LogLine> logLinesRdd = sc.parallelize(logLines);

        JavaPairRDD<String, Map<Integer, List<LogLine>>> groupedRdd = sessionize(logLinesRdd, sessionWindowSeconds);
        groupedRdd.persist(StorageLevel.MEMORY_ONLY());
        double averageSessionLength = computeAverageSessions(computeAverageSessionLengthPerUser(groupedRdd));
        System.out.println("Average Session Length is "+averageSessionLength);
        Map<String, Seconds> maxPerUser = computeMaxSessionLengthPerUser(groupedRdd);
//        for(Map.Entry<String, Seconds> entry : maxPerUser.entrySet())
//        {
//            System.out.println("IP "+entry.getKey()+" MaxSession "+entry.getValue().getSeconds()+" seconds");
//        }
        Map<String, Map<Integer, Integer>> uniqueUrls =  computeUniqueUrlPerUserPerSession(groupedRdd);
        groupedRdd.unpersist();

    }

    private static JavaPairRDD<String, Tuple2<Seconds, Integer>> computeAverageSessionLengthPerUser(JavaPairRDD<String, Map<Integer, List<LogLine>>> input)
    {
        return input.mapToPair(new PairFunction<Tuple2<String,Map<Integer,List<LogLine>>>, String, Tuple2<Seconds, Integer>>() {
            public Tuple2<String, Tuple2<Seconds, Integer>> call(Tuple2<String, Map<Integer, List<LogLine>>> stringMapTuple2) throws Exception {
                Seconds totalSessionLength = Seconds.seconds(0);
                Map<Integer, List<LogLine>> map = stringMapTuple2._2();
                for(Map.Entry<Integer, List<LogLine>> entry : map.entrySet())
                {
                    Seconds sessionLength = getSessionLength(entry.getValue());
                    totalSessionLength = totalSessionLength.plus(sessionLength);
                }
                return new Tuple2(stringMapTuple2._1(), new Tuple2<Seconds, Integer>(totalSessionLength, map.size()));
            }
        });
    }

    private static double computeAverageSessions(JavaPairRDD<String, Tuple2<Seconds, Integer>> data)
    {
        Tuple2<Seconds, Integer> output = data.fold(new Tuple2<String, Tuple2<Seconds, Integer>>("",new Tuple2<Seconds, Integer>(Seconds.seconds(0), 0))
                , new Function2<Tuple2<String, Tuple2<Seconds, Integer>>, Tuple2<String, Tuple2<Seconds, Integer>>, Tuple2<String, Tuple2<Seconds, Integer>>>() {
            public Tuple2<String, Tuple2<Seconds, Integer>> call(Tuple2<String, Tuple2<Seconds, Integer>> stringTuple2Tuple2, Tuple2<String, Tuple2<Seconds, Integer>> stringTuple2Tuple22) throws Exception {
                int numSessions = stringTuple2Tuple2._2()._2()+stringTuple2Tuple22._2()._2();
                Seconds totalLength = stringTuple2Tuple2._2()._1().plus(stringTuple2Tuple22._2()._1());
                return new Tuple2<String, Tuple2<Seconds, Integer>>("",new Tuple2<Seconds, Integer>(totalLength, numSessions));
            }
        })._2();
        return (double)output._1().getSeconds()/output._2();
    }

    private static Map<String, Seconds> computeMaxSessionLengthPerUser(JavaPairRDD<String, Map<Integer, List<LogLine>>> input)
    {
        return input.mapToPair(new PairFunction<Tuple2<String,Map<Integer,List<LogLine>>>, String, Seconds>() {
            public Tuple2<String, Seconds> call(Tuple2<String, Map<Integer, List<LogLine>>> stringMapTuple2) throws Exception {
                Seconds maxSessionLength = Seconds.seconds(0);
                Map<Integer, List<LogLine>> map = stringMapTuple2._2();
                for(Map.Entry<Integer, List<LogLine>> entry : map.entrySet())
                {
                    Seconds sessionLength = getSessionLength(entry.getValue());
                    if(sessionLength.isGreaterThan(maxSessionLength))
                    {
                        maxSessionLength = sessionLength;
                    }
                }
                return new Tuple2(stringMapTuple2._1(), maxSessionLength);
            }
        }).collectAsMap();
    }

    private static Map<String, Map<Integer, Integer>> computeUniqueUrlPerUserPerSession(JavaPairRDD<String, Map<Integer, List<LogLine>>> input)
    {
        return input.mapToPair(new PairFunction<Tuple2<String,Map<Integer,List<LogLine>>>, String, Map<Integer, Integer>>() {
            public Tuple2<String, Map<Integer, Integer>> call(Tuple2<String, Map<Integer, List<LogLine>>> stringMapTuple2) throws Exception {
                Map<Integer, List<LogLine>> map = stringMapTuple2._2();
                Map<Integer, Integer> out = new HashMap<Integer, Integer>();
                for(Map.Entry<Integer, List<LogLine>> entry : map.entrySet())
                {
                    List<LogLine> logs = entry.getValue();
                    int numUnique = getUniqueUrls(logs);
                    out.put(entry.getKey(), numUnique);
                }
                return new Tuple2(stringMapTuple2._1(), out);
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

    private static Seconds getSessionLength(List<LogLine> list)
    {
        DateTime minTime = null;
        DateTime maxTime = null;
        if(list.size()==0)
        {
            return Seconds.seconds(0);
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
        return Seconds.secondsBetween(minTime, maxTime);
        //which way should this go
    }

    private static Map<String, Integer> getUniqueHitsPerUser(JavaRDD<LogLine> logLines)
    {
        return logLines
                .groupBy(new Function<LogLine, String>() {
                    public String call(LogLine logLine) throws Exception {
                        return logLine.getIpAddress();
                    }
                }).mapValues(new Function<Iterable<LogLine>, Integer>() {
                    public Integer call(Iterable<LogLine> logLines) throws Exception {
                        int count = 0;
                        for(LogLine line : logLines)
                        {
                            count++;
                        }
                        return count;
                    }
                }).collectAsMap();
    }
    private static JavaPairRDD<String, Map<Integer, List<LogLine>>> sessionize(JavaRDD<LogLine> logLines, final Seconds sessionSize)
    {
        return logLines
                .groupBy(new Function<LogLine, String>() {
                    public String call(LogLine logLine) throws Exception {
                        return logLine.getIpAddress();
                    }
                }).mapValues(new Function<Iterable<LogLine>, Map<Integer, List<LogLine>>>() {
                     public Map<Integer, List<LogLine>> call(Iterable<LogLine> logLines) throws Exception {
                         List<LogLine> sortedLogLines = new ArrayList<LogLine>();
                         for(LogLine logLine : logLines)
                         {
                             sortedLogLines.add(logLine);
                         }
                         Collections.sort(sortedLogLines, new Comparator<LogLine>() {
                             public int compare(LogLine o1, LogLine o2) {
                                 DateTime dt1 = o1.getDateTime();
                                 DateTime dt2 = o2.getDateTime();
                                 if(dt1.isBefore(dt2))
                                 {
                                     return -1;
                                 }
                                 else if(dt1.isAfter(dt2))
                                 {
                                     return 1;
                                 }
                                 return 0;
                             }
                         });

                        Map<Integer, List<LogLine>> map = new HashMap<Integer, List<LogLine>>();
                        DateTime prevDateTime = null;
                        int counter = 0;
                        for(LogLine logLine : sortedLogLines)
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
                                //Minutes diff = Minutes.minutesBetween(curDateTime, prevDateTime);
                                Seconds diff = Seconds.secondsBetween(curDateTime, prevDateTime);
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