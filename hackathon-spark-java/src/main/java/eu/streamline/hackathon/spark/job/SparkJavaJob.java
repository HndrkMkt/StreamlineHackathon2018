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

        String serializedClassifier = "e    nglish.all.3class.distsim.crf.ser.gz";

        AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifier(serializedClassifier);

//        JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(ctx), new Duration(duration));
        // checkpoint for storing the state
        jssc.checkpoint("checkpoint/");


        // function to store intermediate values in the state
        // it is called in the mapWithState function of DStreamclassifiers/
        Function3<Tuple4<Date, String, String, String>, Optional<Integer>, State<Integer>, Tuple2<Tuple4<Date, String, String, String>, Integer>> mappingFunc =
                new Function3<Tuple4<Date, String, String, String>, Optional<Integer>, State<Integer>, Tuple2<Tuple4<Date, String, String, String>, Integer>>() {
                    @Override
                    public Tuple2<Tuple4<Date, String, String, String>, Integer> call(Tuple4<Date, String, String, String> keyTuple, Optional<Integer> count, State<Integer> state) throws Exception {
                        Integer sum = count.orElse(1) + (state.exists() ? state.get() : 0);
                        Tuple2<Tuple4<Date, String, String, String>, Integer> output = new Tuple2<>(keyTuple, sum);
                        state.update(sum);
                        return output;
                    }
                };

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
                .flatMapToPair(new PairFlatMapFunction<GDELTEvent, Tuple4<Date, String, String, String>, Integer>() {
                    @Override
                    public Iterator<Tuple2<Tuple4<Date, String, String, String>, Integer>> call(GDELTEvent gdeltEvent) {
                        ArrayList<Tuple2<Tuple4<Date, String, String, String>, Integer>> tuples = new ArrayList<>();

                        String firstCountry = (gdeltEvent.actor1Code_countryCode != null ? gdeltEvent.actor1Code_countryCode : "UKNOWN");
                        String secondCountry = (gdeltEvent.actor2Code_countryCode != null ? gdeltEvent.actor2Code_countryCode : "UKNOWN");

                        String firstActor = (gdeltEvent.actor1Code_name != null ? gdeltEvent.actor1Code_name : "UKNOWN");
                        String secondActor = (gdeltEvent.actor2Code_name != null ? gdeltEvent.actor2Code_name : "UKNOWN");

                        Calendar cal = Calendar.getInstance();
                        cal.setTime(gdeltEvent.dateAdded);
                        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                        if (firstCountry.compareTo(secondCountry) < 0) {
                            tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), firstCountry, secondCountry, firstActor), 1));
                            tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), firstCountry, secondCountry, secondActor), 1));
                        } else {
                            tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), secondCountry, firstCountry, firstActor), 1));
                            tuples.add(new Tuple2<>(new Tuple4<>(cal.getTime(), secondCountry, firstCountry, secondActor), 1));
                        }
                        return tuples.iterator();
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer one, Integer two) throws Exception {
                return one + two;
            }
        })
                .mapWithState(StateSpec.function(mappingFunc))
                .mapToPair(new PairFunction<Tuple2<Tuple4<Date, String, String, String>, Integer>, Tuple3<Date, String, String>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<Tuple3<Date, String, String>, Tuple2<String, Integer>> call(Tuple2<Tuple4<Date, String, String, String>, Integer> input) throws Exception {
                        return new Tuple2<>(new Tuple3<>(input._1()._1(), input._1()._2(), input._1()._3()), new Tuple2<>(input._1()._4(), input._2()));
                    }
                })
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
