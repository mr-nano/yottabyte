package com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce;

import com.thoughtworks.yottabyte.DriverTestBase;
import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.RepairParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.hamcrest.beans.SamePropertyValuesAs;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DIFFERENT_CURRENCIES;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DOLLARS;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class RepairCurrencyConversionDriverTest extends DriverTestBase {

  private RepairCurrencyConversionDriver driver;

  private final String TAB_SEPARATOR = "\t";
  private final String OUTPUT_DIR = "output/";

  private RepairParser repairParser;

  @Before
  public void setup() throws IOException {
    configuration = new Configuration();
    configuration.set("fs.defaultFS", "file:///");
    configuration.set("mapred.job.tracker", "local");

    properties = new Properties();
    properties.setProperty(REPAIR_IN_DIFFERENT_CURRENCIES.columnSeparator(), TAB_SEPARATOR);
    properties.setProperty(REPAIR_IN_DOLLARS.path(), OUTPUT_DIR);

    cleanOutputDirectory();

    repairParser = new RepairParser(TAB_SEPARATOR);

    driver = new RepairCurrencyConversionDriver();
  }

  @Test
  public void shouldNotProduceErrorOnDirectoryWithEmptyFiles() throws Exception {
    File emptyRepairDataFile = toFile(new ArrayList<RepairData>(), TAB_SEPARATOR);

    runTestWithInput(emptyRepairDataFile);
    List<Repair> parsedRepairs = repairParser.parse(new File("output/part-m-00000"));

    assertThat(parsedRepairs, hasSize(0));
  }

  @Test
  public void shouldNotModifyAFileWithOnlyRepairsInDollars() throws Exception {
    RepairData dollarRepairData = dummyDollarRepair().build();
    RepairData anotherDollarRepairData = dummyDollarRepair().amount(150.0).build();
    File dollarRepairsDataFile = toFile(Arrays.asList(dollarRepairData, anotherDollarRepairData),
      TAB_SEPARATOR);

    runTestWithInput(dollarRepairsDataFile);
    List<Repair> parsedRepairs = repairParser.parse(new File("output/part-m-00000"));

    assertThat(parsedRepairs,hasSize(2));
    assertThat(parsedRepairs.get(0), new SamePropertyValuesAs(new Repair(dollarRepairData)));
    assertThat(parsedRepairs.get(1), new SamePropertyValuesAs(new Repair(anotherDollarRepairData)));
  }

  @Test
  public void shouldConvertRepairsInDifferentCurrenciesToRepairsInDollars() throws Exception {
    RepairData rupeeRepairData = dummyRupeeRepair().amount(120.0).build();
    RepairData dollarRepairData = dummyDollarRepair().amount(10.0).build();
    File repairsDataFile = toFile(Arrays.asList(rupeeRepairData,dollarRepairData),
      TAB_SEPARATOR);

    runTestWithInput(repairsDataFile);
    List<Repair> parsedRepairs = repairParser.parse(new File("output/part-m-00000"));

    RepairData convertedRupeeRepairData = dummyRupeeRepair()
      .amount(1.92)
      .currency("DOLLARS")
      .build();

    assertThat(parsedRepairs,hasSize(2));
    assertThat(parsedRepairs.get(0), new SamePropertyValuesAs(new Repair(convertedRupeeRepairData)));
    assertThat(parsedRepairs.get(1), new SamePropertyValuesAs(new Repair(dollarRepairData)));
  }

  @Override
  protected List<String> getOutputDirectories() {
    return Arrays.asList(OUTPUT_DIR);
  }

  @Override
  protected Tool getDriver() {
    return driver;
  }

  @Override
  protected void addInputFilesToProperties(List<File> files) {
    for (File file : files) {
      properties.setProperty(REPAIR_IN_DIFFERENT_CURRENCIES.path(), file.getAbsolutePath());
    }
  }
}
