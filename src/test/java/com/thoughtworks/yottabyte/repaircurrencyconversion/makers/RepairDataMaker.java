package com.thoughtworks.yottabyte.repaircurrencyconversion.makers;

import com.natpryce.makeiteasy.Instantiator;
import com.natpryce.makeiteasy.Property;
import com.natpryce.makeiteasy.PropertyLookup;
import com.thoughtworks.yottabyte.datamodels.RepairData;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class RepairDataMaker {

  private static final Logger logger = getLogger(RepairDataMaker.class);


  public static final Property<RepairData,String> vehicleType = new Property<RepairData, String>();
  public static final Property<RepairData,String> code = new Property<RepairData, String>();
  public static final Property<RepairData,String> description = new Property<RepairData, String>();
  public static final Property<RepairData,String> currency = new Property<RepairData, String>();
  public static final Property<RepairData,Double> cost = new Property<RepairData, Double>();

  public static final Instantiator<RepairData> dummyRepair = new Instantiator<RepairData>() {
    @Override
    public RepairData instantiate(PropertyLookup<RepairData> lookup) {
      RepairData repairData = new RepairData();
      repairData.setVehicleType(lookup.valueOf(vehicleType,"dummy vehicle"));
      repairData.setCode(lookup.valueOf(code, "dummy code"));
      repairData.setDescription(lookup.valueOf(description, "dummy description"));
      repairData.setCurrency(lookup.valueOf(currency, "some currency"));
      repairData.setAmount(lookup.valueOf(cost, 0.0));
      return repairData;
    }
  };

  public static final Instantiator<RepairData> dummyRupeeRepair = new Instantiator<RepairData>() {
    @Override
    public RepairData instantiate(PropertyLookup<RepairData> lookup) {
      RepairData repairData = new RepairData();
      repairData.setVehicleType(lookup.valueOf(vehicleType,"dummy vehicle"));
      repairData.setCode(lookup.valueOf(code, "dummy code"));
      repairData.setDescription(lookup.valueOf(description, "dummy description"));
      repairData.setCurrency(lookup.valueOf(currency, "rupee"));
      repairData.setAmount(lookup.valueOf(cost, 0.0));
      return repairData;
    }
  };

  public static final Instantiator<RepairData> dummyDollarRepair = new Instantiator<RepairData>() {
    @Override
    public RepairData instantiate(PropertyLookup<RepairData> lookup) {
      RepairData repairData = new RepairData();
      repairData.setVehicleType(lookup.valueOf(vehicleType,"dummy vehicle"));
      repairData.setCode(lookup.valueOf(code, "dummy code"));
      repairData.setDescription(lookup.valueOf(description, "dummy description"));
      repairData.setCurrency(lookup.valueOf(currency, "DOLLARS"));
      repairData.setAmount(lookup.valueOf(cost, 0.0));
      return repairData;
    }
  };

  public static Text toText(RepairData repairData, String columnSeparator){
    return new Text(repairData.toStringRepresentation(columnSeparator));
  }

  public static File toFile(List<RepairData> repairDataList, String columnSeparator) throws IOException {
    File repairDataFile = File.createTempFile("repair",".tmp");
    try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(repairDataFile))){
      for (RepairData repairData : repairDataList) {
        bufferedWriter.write(repairData.toStringRepresentation(columnSeparator));
        bufferedWriter.newLine();
      }
    }
    logger.info("Created temporary file at: " + repairDataFile.getAbsolutePath());
    repairDataFile.deleteOnExit();
    return repairDataFile;
  }
}
