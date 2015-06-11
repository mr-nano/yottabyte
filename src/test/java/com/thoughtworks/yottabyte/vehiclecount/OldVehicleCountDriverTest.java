package com.thoughtworks.yottabyte.vehiclecount;

import com.thoughtworks.yottabyte.DriverTestBase;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.OLD_VEHICLES;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.VEHICLES;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.toFile;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.veryOldVehicleData;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.VehicleDataBuilders.youngVehicleData;
import static java.util.Arrays.asList;
import static org.apache.commons.io.IOUtils.readLines;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class OldVehicleCountDriverTest extends DriverTestBase {

  private OldVehicleCountDriver driver;

  private final String TAB_SEPARATOR = "\t";
  private final String OUTPUT_DIR = "output/";
  private final String REFERENCE_DATE = "2015-01-01";
  private final String REFERENCE_DATE_FORMAT = "yyyy-MM-dd";
  private final String VEHICLE_DATE_FORMAT = "yyyy-MM-dd";

  @Before
  public void setup() throws IOException {
    configuration = new Configuration();
    configuration.set("fs.defaultFS", "file:///");
    configuration.set("mapred.job.tracker", "local");

    properties = new Properties();
    properties.setProperty(VEHICLES.columnSeparator(), TAB_SEPARATOR);
    properties.setProperty(VEHICLES.referenceDate(), REFERENCE_DATE);
    properties.setProperty(VEHICLES.referenceDateFormat(), REFERENCE_DATE_FORMAT);
    properties.setProperty(VEHICLES.dateFormat(), VEHICLE_DATE_FORMAT);
    properties.setProperty(OLD_VEHICLES.path(), OUTPUT_DIR);

    cleanOutputDirectory();

    driver = new OldVehicleCountDriver();
  }

  @Test
  public void shouldNotProduceErrorOnDirectoryWithEmptyFiles() throws Exception {
    File emptyVehicleDataFile = toFile(new ArrayList<VehicleData>(), TAB_SEPARATOR);

    runTestWithInput(emptyVehicleDataFile);
    List<String> outputs = readLines(new FileReader("output/part-r-00000"));

    assertThat(outputs, hasSize(0));
  }

  @Test
  public void shouldExcludeYoungVehiclesFromCounting() throws Exception {

    VehicleData youngVehicleOne = youngVehicleData().build();
    VehicleData youngVehicleTwo = youngVehicleData().registrationNumber("different").build();
    File youngVehicleDataFile = toFile(asList(youngVehicleOne,youngVehicleTwo), TAB_SEPARATOR);

    runTestWithInput(youngVehicleDataFile);

    List<String> outputs = readLines(new FileReader("output/part-r-00000"));

    assertThat(outputs, hasSize(0));
  }

  @Test
  public void shouldIncludeOldVehiclesInCounting() throws Exception {

    VehicleData oldVehicleData = veryOldVehicleData().type("truck").build();
    VehicleData youngVehicleTwo = youngVehicleData().registrationNumber("different").build();
    File vehiclesDataFile = toFile(asList(oldVehicleData,youngVehicleTwo), TAB_SEPARATOR);

    runTestWithInput(vehiclesDataFile);

    List<String> outputs = readLines(new FileReader("output/part-r-00000"));

    assertThat(outputs, hasSize(1));
    assertThat(outputs.get(0),containsString(" 1 "));
    assertThat(outputs.get(0),containsString("truck"));
  }

  @Test
  public void shouldAddAllOldVehiclesInCounting() throws Exception {

    VehicleData oldTruckOne = veryOldVehicleData().type("truck").build();
    VehicleData oldTruckTwo = veryOldVehicleData().type("truck").registrationNumber("another truck").build();

    VehicleData oldBusOne = veryOldVehicleData().type("bus").registrationNumber("a bus").build();
    VehicleData oldBusTwo = veryOldVehicleData().type("bus").registrationNumber("a different bus").build();
    VehicleData oldBusThree = veryOldVehicleData().type("bus").registrationNumber("very different bus").build();


    VehicleData youngTruckOne = youngVehicleData().type("truck").registrationNumber("young truck").build();
    File vehiclesDataFile = toFile(asList(oldTruckOne,oldTruckTwo,oldBusOne,oldBusTwo,youngTruckOne,oldBusThree), TAB_SEPARATOR);

    runTestWithInput(vehiclesDataFile);

    List<String> outputs = readLines(new FileReader("output/part-r-00000"));

    assertThat(outputs, hasSize(2));
    assertThat(outputs.get(0),containsString(" 3 "));
    assertThat(outputs.get(0),containsString("bus"));

    assertThat(outputs.get(1),containsString(" 2 "));
    assertThat(outputs.get(1),containsString("truck"));
  }

  @Override
  protected List<String> getOutputDirectories() {
    return asList(OUTPUT_DIR);
  }

  @Override
  protected Tool getDriver() {
    return driver;
  }

  @Override
  protected void addInputFilesToProperties(List<File> files) {
    for (File file : files) {
      properties.setProperty(VEHICLES.path(), file.getAbsolutePath());
    }
  }
}