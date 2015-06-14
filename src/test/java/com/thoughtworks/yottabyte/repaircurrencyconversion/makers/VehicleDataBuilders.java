package com.thoughtworks.yottabyte.repaircurrencyconversion.makers;

import com.thoughtworks.yottabyte.datamodels.VehicleData;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.joda.time.format.DateTimeFormat.forPattern;
import static org.slf4j.LoggerFactory.getLogger;

public class VehicleDataBuilders {

  public static final String vehicleDateTimePattern = "yyyy-MM-dd";
  private static final Logger logger = getLogger(RepairDataBuilders.class);

  public static VehicleData.VehicleDataBuilder veryOldVehicleData(){
    return VehicleData
      .builder()
      .dateTimeFormat(vehicleDateTimePattern)
      .date(forPattern(vehicleDateTimePattern).parseDateTime("1975-09-01"))
      .owner("Someone")
      .registrationNumber("Some registration number")
      .type("car");
  }

  public static VehicleData.VehicleDataBuilder youngVehicleData(){
    return VehicleDataBuilders
      .veryOldVehicleData()
      .registrationNumber("some other registration number")
      .owner("some other owner")
      .date(forPattern(vehicleDateTimePattern).parseDateTime("2015-01-01"));
  }

  public static VehicleData.VehicleDataBuilder someVehicleData(){
    return veryOldVehicleData();
  }

  public static Text toText(VehicleData vehicleData, String columnSeparator){
    return new Text(vehicleData.toStringRepresentation(columnSeparator));
  }

  public static File toFile(List<VehicleData> vehicleDataList, String columnSeparator) throws IOException {
    File vehicle = File.createTempFile("vehicle",".tmp");
    try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(vehicle))){
      for (VehicleData vehicleData : vehicleDataList) {
        bufferedWriter.write(vehicleData.toStringRepresentation(columnSeparator));
        bufferedWriter.newLine();
      }
    }
    logger.info("Created temporary file at: " + vehicle.getAbsolutePath());
    vehicle.deleteOnExit();
    return vehicle;
  }
}
