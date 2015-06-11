package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataBuilders;
import org.apache.hadoop.io.Text;
import org.hamcrest.beans.SamePropertyValuesAs;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;


public class RepairParserTest {

  private String columnSeparator = ",";

  private RepairData dollarRepairData;
  private RepairData rupeeRepairData;

  private RepairParser repairParser;

  @Before
  public void setup(){
    repairParser = new RepairParser(columnSeparator);
    dollarRepairData = dummyDollarRepair().build();
    rupeeRepairData = dummyRupeeRepair().build();
  }

  @Test
  public void shouldParseFromText() throws Exception {
    Text dollarRepairText = toText(dollarRepairData, columnSeparator);
    Repair parsedRepair = repairParser.parse(dollarRepairText);
    Repair expectedRepair = new Repair(dummyDollarRepair().build());

    assertThat(parsedRepair, new SamePropertyValuesAs(expectedRepair));
  }

  @Test
  public void shouldParseFromString(){
    String dollarRepairString = toText(dollarRepairData,columnSeparator).toString();
    Repair parsedRepair = repairParser.parse(dollarRepairString);
    Repair expectedRepair = new Repair(dummyDollarRepair().build());

    assertThat(parsedRepair, new SamePropertyValuesAs(expectedRepair));
  }

  @Test
  public void shouldParseFromAFile() throws IOException {
    List<RepairData> repairDataList = Arrays.asList(dollarRepairData,rupeeRepairData);
    List<Repair> expectedRepairs = Arrays.asList(new Repair(dollarRepairData),
      new Repair(rupeeRepairData));
    File repairFile = RepairDataBuilders.toFile(repairDataList, columnSeparator);
    List<Repair> parsedRepairs = repairParser.parse(repairFile);

    assertThat(parsedRepairs, hasSize(2));
    assertThat(parsedRepairs.get(0),new SamePropertyValuesAs<>(new Repair(dollarRepairData)));
    assertThat(parsedRepairs.get(1),new SamePropertyValuesAs<>(new Repair(rupeeRepairData)));
  }

  @Test
  public void shouldReturnEmptyListOfRepairWhenParsingAnEmptyFile() throws IOException {
    File repairFile = RepairDataBuilders.toFile(new ArrayList<RepairData>(), ",");
    assertThat(repairParser.parse(repairFile),hasSize(0));
  }

}
