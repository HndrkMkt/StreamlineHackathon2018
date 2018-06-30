package eu.streamline.hackathon.spark.job;

import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.spark.scala.operations.GDELTInputReceiver;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author behrouz
 */
public class SparkJavaJob {

    public static void main(String[] args) throws InterruptedException {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String pathToGDELT = params.get("path");
        final Long duration = params.getLong("micro-batch-duration", 1000);
//        final String country = params.get("country", "USA");

        SparkConf conf = new SparkConf().setAppName("Spark Java GDELT Analyzer");
        String masterURL = conf.get("spark.master", "local[*]");
        conf.setMaster(masterURL);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(duration));
        // checkpoint for storing the state
        jssc.checkpoint("checkpoint/");

        // function to store intermediate values in the state
        // it is called in the mapWithState function of DStream
        Function3<Tuple3<Date, String, String>, Optional<Double>, State<Double>, Tuple2<Tuple3<Date, String, String>, Double>> mappingFunc =
                new Function3<Tuple3<Date, String, String>, Optional<Double>, State<Double>, Tuple2<Tuple3<Date, String, String>, Double>>() {
                    @Override
                    public Tuple2<Tuple3<Date, String, String>, Double> call(Tuple3<Date, String, String> keyTuple, Optional<Double> avgTone, State<Double> state) throws Exception {
                        Double sum = avgTone.orElse(0.0) + (state.exists() ? state.get() : 0.0);
                        Tuple2<Tuple3<Date, String, String>, Double> output = new Tuple2<>(keyTuple, sum);
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
                .mapToPair(new PairFunction<GDELTEvent, Tuple3<Date, String, String>, Double>() {
                    @Override
                    public Tuple2<Tuple3<Date, String, String>, Double> call(GDELTEvent gdeltEvent) throws Exception {
                        String firstCountry = (gdeltEvent.actor1Code_countryCode != null ? gdeltEvent.actor1Code_countryCode : "UKNOWN");
                        String secondCountry = (gdeltEvent.actor2Code_countryCode != null ? gdeltEvent.actor2Code_countryCode : "UKNOWN");
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(gdeltEvent.dateAdded);
                        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                        if (firstCountry.compareTo(secondCountry) < 0) {
                            return new Tuple2<>(
                                    new Tuple3<>(cal.getTime(), firstCountry, secondCountry),
                                    gdeltEvent.avgTone);
                        } else {
                            return new Tuple2<>(
                                    new Tuple3<>(cal.getTime(), secondCountry, firstCountry),
                                    gdeltEvent.avgTone);
                        }
                    }
                })
                .reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double one, Double two) throws Exception {
                        return one + two;
                    }
                })
                .mapWithState(StateSpec.function(mappingFunc))
                .map(new Function<Tuple2<Tuple3<Date, String, String>, Double>, String>() {
                    @Override
                    public String call(Tuple2<Tuple3<Date, String, String>, Double> event) throws Exception {
                        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                        return "Country1(" + event._1()._2() + "), Country2(" + event._1()._3() + "), Week(" + format.format(event._1()._1()) + "), " +
                                "AvgTone(" + event._2() + ")";

                    }
                })
                .print();

        jssc.start();
        jssc.awaitTermination();

    }
}
