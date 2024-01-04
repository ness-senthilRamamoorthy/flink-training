package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;

import java.util.Properties;

/**
 * The Ride Cleansing exercise from the Flink training.
 *
 * <p>The task of this exercise is to filter a data stream of taxi ride records to keep only rides
 * that both start and end within New York City. The resulting stream should be printed.
 */
public class RideCleansingMapExercise {

    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<EnrichedRide> sink;

    /**
     * Creates a job using the source and sink provided.
     */
    public RideCleansingMapExercise(SourceFunction<TaxiRide> source, SinkFunction<EnrichedRide> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        RideCleansingMapExercise job =
                new RideCleansingMapExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> taxiRideDataStream = env.addSource(source);

        DataStream<EnrichedRide> enrichedRideDataStream = taxiRideDataStream.map(new Enrichment());

        enrichedRideDataStream.addSink(sink);


        // run the pipeline and return the result
        return env.execute("Taxi Ride Cleansing Map");
    }


    public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {
        @Override
        public EnrichedRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichedRide(taxiRide);
        }
    }
}