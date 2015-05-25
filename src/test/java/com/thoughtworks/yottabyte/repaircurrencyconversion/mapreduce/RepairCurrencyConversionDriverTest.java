package com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce;

import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.RepairParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.beans.SamePropertyValuesAs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.natpryce.makeiteasy.MakeItEasy.a;
import static com.natpryce.makeiteasy.MakeItEasy.make;
import static com.natpryce.makeiteasy.MakeItEasy.with;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DIFFERENT_CURRENCIES;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DOLLARS;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataMaker.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class RepairCurrencyConversionDriverTest {

  private Configuration configuration;
  private Properties properties;

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

  @After
  public void cleanup() throws IOException {
    cleanOutputDirectory();
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
    RepairData dollarRepairData = make(a(dummyDollarRepair));
    RepairData anotherDollarRepairData = make(a(dummyDollarRepair,with(cost,150.0)));
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
    RepairData rupeeRepairData = make(a(dummyRupeeRepair,with(cost,120.0)));
    RepairData dollarRepairData = make(a(dummyDollarRepair,with(cost,10.0)));
    File repairsDataFile = toFile(Arrays.asList(rupeeRepairData,dollarRepairData),
      TAB_SEPARATOR);

    runTestWithInput(repairsDataFile);
    List<Repair> parsedRepairs = repairParser.parse(new File("output/part-m-00000"));

    RepairData convertedRupeeRepairData = make(a(dummyRupeeRepair,
      with(cost,1.92),
      with(currency,"DOLLARS")));

    assertThat(parsedRepairs,hasSize(2));
    assertThat(parsedRepairs.get(0), new SamePropertyValuesAs(new Repair(convertedRupeeRepairData)));
    assertThat(parsedRepairs.get(1), new SamePropertyValuesAs(new Repair(dollarRepairData)));
  }

  private File makeTemporaryPropertiesFile() throws IOException {
    File tempPropertiesFile = File.createTempFile("properties", "config");
    try (FileOutputStream fos = new FileOutputStream(tempPropertiesFile)) {
      properties.store(fos, null);
    }
    return tempPropertiesFile;
  }

  private void cleanOutputDirectory() throws IOException {
    FileSystem fileSystem = FileSystem.getLocal(configuration);
    fileSystem.delete(new Path(OUTPUT_DIR), true);
  }

  private void addInputFileToProperties(File file) {
    properties.setProperty(REPAIR_IN_DIFFERENT_CURRENCIES.path(), file.getAbsolutePath());
  }

  private void runTest() throws Exception {
    driver.setConf(configuration);
    driver.run(new String[]{makeTemporaryPropertiesFile().getAbsolutePath()});
  }

  private void runTestWithInput(File inputFile) throws Exception {
    addInputFileToProperties(inputFile);
    runTest();
  }


}
