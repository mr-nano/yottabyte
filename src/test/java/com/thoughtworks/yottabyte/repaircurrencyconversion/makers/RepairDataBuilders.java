package com.thoughtworks.yottabyte.repaircurrencyconversion.makers;

import com.thoughtworks.yottabyte.datamodels.RepairData;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class RepairDataBuilders {

  private static final Logger logger = getLogger(RepairDataBuilders.class);

  public static RepairData.RepairDataBuilder dummyRepair(){
    return RepairData
      .builder()
      .vehicleType("dummy vehicle")
      .code("dummy code")
      .description("dummy description")
      .currency("some currency")
      .amount(0.0);
  }

  public static RepairData.RepairDataBuilder dummyRupeeRepair(){
    return dummyRepair()
      .currency("rupee");
  }

  public static RepairData.RepairDataBuilder dummyDollarRepair(){
    return dummyRepair()
      .currency("DOLLARS");
  }

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
