package eu.streamline.hackathon.spark.job;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.spark.scala.operations.GDELTInputReceiver;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

import edu.stanford.nlp.sequences.DocumentReaderAndWriter;
import scala.Tuple4;

/**
 * @author behrouz
 */
public class SparkJavaJob {

    public static void main(String[] args) throws Exception {


        ParameterTool params = ParameterTool.fromArgs(args);
        final String pathToGDELT = params.get("path");
        final Long duration = params.getLong("micro-batch-duration", 1000);
//        final String country = params.get("country", "USA");

        SparkConf conf = new SparkConf().setAppName("Spark Java GDELT Analyzer");
        String masterURL = conf.get("spark.master", "local[*]");
        conf.setMaster(masterURL);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(duration));

//        SparkContext ctx = SparkContext.getOrCreate(conf);
//        ctx.addFile("/home/hendrik/projects/tu_berlin/dima/bdml/streamline-hackathon-boilerplate/english.all.3class.distsim.crf.ser.gz");
//

        String serializedClassifier = "english.all.3class.distsim.crf.ser.gz";

        AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifier(serializedClassifier);

//        JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(ctx), new Duration(duration));
        // checkpoint for storing the state
        jssc.checkpoint("checkpoint/");


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

//        Function<Iterable<Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>> sortingFunct =
//                new Function<Iterable<Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>() {
//                    @Override
//                    public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> input) throws Exception {
//                        ArrayList<Tuple2<String, Integer>> tuples = new ArrayList<>();
//                        for (Tuple2<String, Integer> tuple : input) {
//                            tuples.add(tuple);
//                        }
//                        tuples.sort(new Comparator);
//                        Integer sum = count.orElse(1) + (state.exists() ? state.get() : 0);
//                        Tuple2<Tuple4<Date, String, String, String>, Integer> output = new Tuple2<>(keyTuple, sum);
//                        state.update(sum);
//                        return output;
//                    }
//                };

        jssc
                .receiverStream(new GDELTInputReceiver(pathToGDELT))
//                .filter(new Function<GDELTEvent, Boolean>() {
//                    @Override
//                    public Boolean call(GDELTEvent gdeltEvent) throws Exception {
//                        return Objects.equals(gdeltEvent.actor1Code_countryCode, country);
//                    }
//                })
//                .mapToPair(new PairFunction<GDELTEvent, Tuple3<Date, String, String>, Tuple2<String, String>>() {
//                    @Override
//                    public Tuple2<Tuple3<Date, String, String>, Tuple2<String, String>> call(GDELTEvent gdeltEvent) throws Exception {
//                        String firstCountry = (gdeltEvent.actor1Code_countryCode != null ? gdeltEvent.actor1Code_countryCode : "UKNOWN");
//                        String secondCountry = (gdeltEvent.actor2Code_countryCode != null ? gdeltEvent.actor2Code_countryCode : "UKNOWN");
//
//                        String firstActor = (gdeltEvent.actor1Code_name != null ? gdeltEvent.actor1Code_name : "UKNOWN");
//                        String secondActor = (gdeltEvent.actor2Code_name != null ? gdeltEvent.actor2Code_name : "UKNOWN");
//                        Calendar cal = Calendar.getInstance();
//                        cal.setTime(gdeltEvent.dateAdded);
//                        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
//                        if (firstCountry.compareTo(secondCountry) < 0) {
//                            return new Tuple2<>(
//                                    new Tuple3<>(cal.getTime(), firstCountry, secondCountry),
//                                    new Tuple2<>(firstActor, secondActor));
//                        } else {
//                            return new Tuple2<>(
//                                    new Tuple3<>(cal.getTime(), secondCountry, firstCountry),
//                                    new Tuple2<>(secondActor, firstActor));
//                        }
//                    }
//                })
                .flatMapToPair(new PairFlatMapFunction<GDELTEvent, Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>() {
                    @Override
                    public Iterator<Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>> call(GDELTEvent gdeltEvent) {
                        ArrayList<Tuple2<Tuple4<Date, String, String, String>, Tuple2<Integer, Double>>> tuples = new ArrayList<>();

                        String firstCountry = (gdeltEvent.actor1Code_countryCode != null ? gdeltEvent.actor1Code_countryCode : "UKNOWN");
                        String secondCountry = (gdeltEvent.actor2Code_countryCode != null ? gdeltEvent.actor2Code_countryCode : "UKNOWN");

                        String firstActor = (gdeltEvent.actor1Code_name != null ? gdeltEvent.actor1Code_name : "UKNOWN");
                        String secondActor = (gdeltEvent.actor2Code_name != null ? gdeltEvent.actor2Code_name : "UKNOWN");

                        Calendar cal = Calendar.getInstance();
                        cal.setTime(gdeltEvent.dateAdded);
                        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                        Tuple2<Integer, Double> value = new Tuple2<>(1, gdeltEvent.avgTone);
                        if (firstCountry.compareTo(secondCountry) < 0) {
                            tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), firstCountry, secondCountry, firstActor), value));
                            tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), firstCountry, secondCountry, secondActor), value));
                        } else {
                            tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), secondCountry, firstCountry, firstActor), value));
                            tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), secondCountry, firstCountry, secondActor), value));
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

//                .mapValues()
//
//                .map(new Function<Tuple2<Tuple4<Date, String, String>, Integer>, String>() {
//                    @Override
//                    public String call(Tuple2<Tuple4<Date, String, String, String>, Integer> event) throws Exception {
//                        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//                        return "Country1(" + event._1()._2() + "), Country2(" + event._1()._3() + "), , Actor(" + event._1()._4() + "), Week(" + format.format(event._1()._1()) + "), " +
//                                "Count(" + event._2() + ")";
//
//                    }
//                })
                .print();

        jssc.start();
        jssc.awaitTermination();

    }
}
