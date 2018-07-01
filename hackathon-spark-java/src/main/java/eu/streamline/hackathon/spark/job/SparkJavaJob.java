package eu.streamline.hackathon.spark.job;

import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.spark.scala.operations.GDELTInputReceiver;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import scala.Tuple4;

/**
 * @author behrouz
 */
public class SparkJavaJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String pathToGDELT = params.get("path");
        final Long duration = params.getLong("micro-batch-duration", 1000);

        SparkConf conf = new SparkConf().setAppName("Spark Java GDELT Analyzer");
        String masterURL = conf.get("spark.master", "local[*]");
        conf.setMaster(masterURL);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(duration));

        // checkpoint for storing the state
        jssc.checkpoint("checkpoint/");

        JavaReceiverInputDStream<GDELTEvent> receiverStream = jssc
                .receiverStream(new GDELTInputReceiver(pathToGDELT));

        addActorTypeStream(receiverStream);

        addBilateralToneStream(receiverStream);

        jssc.start();
        jssc.awaitTermination();

    }

    private static void addBilateralToneStream(JavaReceiverInputDStream<GDELTEvent> receiverStream) {
        // function to store intermediate values in the state
        // it is called in the mapWithState function of DStream
        Function3<Tuple3<Date, String, String>, Optional<Tuple2<Integer, Double>>, State<Tuple2<Integer, Double>>, Tuple2<Tuple3<Date, String, String>, Tuple2<Integer, Double>>> mappingFunc =
                new Function3<Tuple3<Date, String, String>, Optional<Tuple2<Integer, Double>>, State<Tuple2<Integer, Double>>, Tuple2<Tuple3<Date, String, String>, Tuple2<Integer, Double>>>() {
                    @Override
                    public Tuple2<Tuple3<Date, String, String>, Tuple2<Integer, Double>> call(Tuple3<Date, String, String> keyTuple, Optional<Tuple2<Integer, Double>> optValue, State<Tuple2<Integer, Double>> state) throws Exception {
                        Tuple2<Integer, Double> value = optValue.orElse(new Tuple2<>(1, 0.));
                        Tuple2<Integer, Double> stateValue = (state.exists() ? state.get() : new Tuple2<>(0, 0.));
                        Tuple2<Integer, Double> resultState = new Tuple2<>(stateValue._1() + value._1(), stateValue._2() + value._2());
                        Tuple2<Tuple3<Date, String, String>, Tuple2<Integer, Double>> output = new Tuple2<>(keyTuple, resultState);
                        state.update(resultState);
                        return output;
                    }
                };

        receiverStream.flatMapToPair((new PairFlatMapFunction<GDELTEvent, Tuple3<Date, String, String>, Tuple2<Integer, Double>>() {
            @Override
            public Iterator<Tuple2<Tuple3<Date, String, String>, Tuple2<Integer, Double>>> call(GDELTEvent gdeltEvent) throws Exception {
                ArrayList<Tuple2<Tuple3<Date, String, String>, Tuple2<Integer, Double>>> tuples = new ArrayList<>();
                String firstCountry, secondCountry;
                if (gdeltEvent.actor1Geo_countryCode != null) {
                    firstCountry = gdeltEvent.actor1Geo_countryCode;
                } else {
                    return tuples.iterator();
                }
                if (gdeltEvent.actor2Geo_countryCode != null) {
                    secondCountry = gdeltEvent.actor2Geo_countryCode;
                } else {
                    return tuples.iterator();
                }
                Calendar cal = Calendar.getInstance();
                cal.setTime(gdeltEvent.dateAdded);
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                tuples.add(new Tuple2<>(
                        new Tuple3<>(cal.getTime(), firstCountry, secondCountry),
                        new Tuple2<>(1, gdeltEvent.avgTone)));
                tuples.add(new Tuple2<>(
                        new Tuple3<>(cal.getTime(), secondCountry, firstCountry),
                        new Tuple2<>(1, gdeltEvent.avgTone)));
                return tuples.iterator();
            }
        }))
                .reduceByKey(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
                    @Override
                    public Tuple2<Integer, Double> call(Tuple2<Integer, Double> one, Tuple2<Integer, Double> two) throws Exception {
                        return new Tuple2<>(one._1() + two._1(), one._2() + two._2());
                    }
                })
                .mapWithState(StateSpec.function(mappingFunc))
                .map(new Function<Tuple2<Tuple3<Date, String, String>, Tuple2<Integer, Double>>, String>() {
                    @Override
                    public String call(Tuple2<Tuple3<Date, String, String>, Tuple2<Integer, Double>> event) throws Exception {
                        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                        return "" + event._1()._2() + ";" + event._1()._3() + ";" + format.format(event._1()._1()) + ";" +
                                event._2()._2() / event._2()._1();
                    }
                })
                .dstream().saveAsTextFiles("output/bilateral_relationships", "");
    }

    private static void addActorTypeStream(JavaReceiverInputDStream<GDELTEvent> receiverStream) {

        // function to store intermediate values in the state
        // it is called in the mapWithState function of DStreamclassifiers/
        Function3<Tuple4<Date, String, String, String>, Optional<Tuple2<Integer, Double>>, State<Tuple2<Integer, Double>>, Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>> mappingFunc =
                new Function3<Tuple4<Date, String, String, String>, Optional<Tuple2<Integer, Double>>, State<Tuple2<Integer, Double>>, Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>>() {
                    @Override
                    public Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>> call(Tuple4<Date, String, String, String> keyTuple, Optional<Tuple2<Integer, Double>> optValue, State<Tuple2<Integer, Double>> state) throws Exception {
                        Tuple2<Integer, Double> value = optValue.orElse(new Tuple2<>(1, 0.));
                        Tuple2<Integer, Double> stateValue = (state.exists() ? state.get() : new Tuple2<>(0, 0.));
                        Tuple2<Integer, Double> resultState = new Tuple2<>(stateValue._1() + value._1(), stateValue._2() + value._2());
                        Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>> output = new Tuple2<>(keyTuple, resultState);
                        state.update(resultState);
                        return output;
                    }
                };

        Function<List<Tuple3<String, Integer, Double>>, List<Tuple3<String, Integer, Double>>> sortingFunc =
                new Function<List<Tuple3<String, Integer, Double>>, List<Tuple3<String, Integer, Double>>>() {
                    @Override
                    public List<Tuple3<String, Integer, Double>> call(List<Tuple3<String, Integer, Double>> inputList) {
                        Collections.sort(inputList, new Comparator<Tuple3<String, Integer, Double>>() {
                            @Override
                            public int compare(Tuple3<String, Integer, Double> o1, Tuple3<String, Integer, Double> o2) {
                                return -o1._2().compareTo(o2._2());
                            }
                        });
                        inputList = new ArrayList<>(inputList.subList(0, Math.min(5, inputList.size())));
                        return inputList;
                    }
                };

        receiverStream.flatMapToPair(new PairFlatMapFunction<GDELTEvent, Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>() {
            @Override
            public Iterator<Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>> call(GDELTEvent gdeltEvent) {
                ArrayList<Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>> tuples = new ArrayList<>();

                String firstCountry = (gdeltEvent.actor1Geo_countryCode != null ? gdeltEvent.actor1Geo_countryCode : "UKNOWN");
                String secondCountry = (gdeltEvent.actor2Geo_countryCode != null ? gdeltEvent.actor2Geo_countryCode : "UKNOWN");

                Calendar cal = Calendar.getInstance();
                cal.setTime(gdeltEvent.dateAdded);
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                Tuple2<Integer, Double> value = new Tuple2<>(1, gdeltEvent.avgTone);

                LinkedList<String> actorTypes = new LinkedList<>();

                if (gdeltEvent.actor1Code_type1Code != null) {
                    actorTypes.add(gdeltEvent.actor1Code_type1Code);
                }
                if (gdeltEvent.actor1Code_type2Code != null) {
                    actorTypes.add(gdeltEvent.actor1Code_type2Code);
                }
                if (gdeltEvent.actor1Code_type3Code != null) {
                    actorTypes.add(gdeltEvent.actor1Code_type3Code);
                }
                if (gdeltEvent.actor2Code_type1Code != null) {
                    actorTypes.add(gdeltEvent.actor2Code_type1Code);
                }
                if (gdeltEvent.actor2Code_type2Code != null) {
                    actorTypes.add(gdeltEvent.actor2Code_type2Code);
                }
                if (gdeltEvent.actor2Code_type3Code != null) {
                    actorTypes.add(gdeltEvent.actor2Code_type3Code);
                }
                for (String actorType: actorTypes) {
                    tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), firstCountry, secondCountry, actorType), value));
                    tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), secondCountry, firstCountry, actorType), value));
                }
                return tuples.iterator();
            }
        }).reduceByKey(new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Double> one, Tuple2<Integer, Double> two) throws Exception {
                return new Tuple2<>(one._1() + two._1(), one._2() + two._2());
            }
        })
                .mapWithState(StateSpec.function(mappingFunc))
                .mapToPair(new PairFunction<Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>, Tuple3<Date, String, String>, List<Tuple3<String, Integer, Double>>>() {
                    @Override
                    public Tuple2<Tuple3<Date, String, String>, List<Tuple3<String, Integer, Double>>> call(Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>> input) throws Exception {
                        List<Tuple3<String, Integer, Double>> value = new ArrayList<>();
                        value.add(new Tuple3<>(input._1()._4(), input._2()._1(), input._2()._2() / input._2()._1()));
                        return new Tuple2<>(new Tuple3<>(input._1()._1(), input._1()._2(), input._1()._3()), value);
                    }
                }).reduceByKey(new Function2<List<Tuple3<String, Integer, Double>>, List<Tuple3<String, Integer, Double>>, List<Tuple3<String, Integer, Double>>>() {
            @Override
            public List<Tuple3<String, Integer, Double>> call(List<Tuple3<String, Integer, Double>> one, List<Tuple3<String, Integer, Double>> two) throws Exception {
                ArrayList<Tuple3<String, Integer, Double>> newList = new ArrayList<>();
                newList.addAll(one);
                newList.addAll(two);
                return newList;
            }
        })
                .mapValues(sortingFunc)
                .map(new Function<Tuple2<Tuple3<Date, String, String>, List<Tuple3<String, Integer, Double>>>, String>() {
                    @Override
                    public String call(Tuple2<Tuple3<Date, String, String>, List<Tuple3<String, Integer, Double>>> event) throws Exception {
                        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                        return "" + event._1()._2() + ";" + event._1()._3() + ";" + format.format(event._1()._1()) + ";" +
                                event._2().toString();
                    }
                })
                .dstream().saveAsTextFiles("output/week", "");
    }
}
