package org.apache.flink.training.exercises.common.datatypes;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

@Data
@NoArgsConstructor
public class EnrichedRide extends TaxiRide {
    private int startCell;
    private int endCell;

    public EnrichedRide(TaxiRide taxiRide) {
        super(taxiRide.rideId, taxiRide.isStart);
        this.startCell = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
        this.endCell = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
    }
}