package com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.RepairParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.domain.currency.Currency.DOLLARS;

public class RepairCurrencyConversionMapper extends Mapper<Object, Text, NullWritable, Text> {

  private Configuration configuration;

  public static final String COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  private RepairParser repairParser;
  private String columnSeparator;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
    columnSeparator = get(COLUMN_SEPARATOR);
    repairParser = new RepairParser(columnSeparator);
  }

  @Override
  public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
    Repair repair = repairParser.parse(row);
    repair.convertTo(DOLLARS);
    context.write(NullWritable.get(),
      new Text(repair.toStringRepresentation(columnSeparator)));
  }

  protected String get(String key){
    return Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
  }

}
