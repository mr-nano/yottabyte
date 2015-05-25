package com.thoughtworks.yottabyte.repaircurrencyconversion.domain;

import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataMaker;
import org.apache.hadoop.io.Text;
import org.hamcrest.beans.SamePropertyValuesAs;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.natpryce.makeiteasy.MakeItEasy.a;
import static com.natpryce.makeiteasy.MakeItEasy.make;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.makers.RepairDataMaker.*;
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
    dollarRepairData = make(a(dummyDollarRepair));
    rupeeRepairData = make(a(dummyRupeeRepair));
  }

  @Test
  public void shouldParseFromText() throws Exception {
    Text dollarRepairText = toText(dollarRepairData, columnSeparator);
    Repair parsedRepair = repairParser.parse(dollarRepairText);
    Repair expectedRepair = new Repair(make(a(dummyDollarRepair)));

    assertThat(parsedRepair, new SamePropertyValuesAs(expectedRepair));
  }

  @Test
  public void shouldParseFromString(){
    String dollarRepairString = toText(dollarRepairData,columnSeparator).toString();
    Repair parsedRepair = repairParser.parse(dollarRepairString);
    Repair expectedRepair = new Repair(make(a(dummyDollarRepair)));

    assertThat(parsedRepair, new SamePropertyValuesAs(expectedRepair));
  }

  @Test
  public void shouldParseFromAFile() throws IOException {
    List<RepairData> repairDataList = Arrays.asList(dollarRepairData,rupeeRepairData);
    List<Repair> expectedRepairs = Arrays.asList(new Repair(dollarRepairData),
      new Repair(rupeeRepairData));
    File repairFile = RepairDataMaker.toFile(repairDataList, columnSeparator);
    List<Repair> parsedRepairs = repairParser.parse(repairFile);

    assertThat(parsedRepairs, hasSize(2));
    assertThat(parsedRepairs.get(0),new SamePropertyValuesAs<>(new Repair(dollarRepairData)));
    assertThat(parsedRepairs.get(1),new SamePropertyValuesAs<>(new Repair(rupeeRepairData)));
  }

  @Test
  public void shouldReturnEmptyListOfRepairWhenParsingAnEmptyFile() throws IOException {
    File repairFile = RepairDataMaker.toFile(new ArrayList<RepairData>(),",");
    assertThat(repairParser.parse(repairFile),hasSize(0));
  }

}
