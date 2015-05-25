package com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce;

import com.thoughtworks.yottabyte.ConfiguredMapper;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.RepairParser;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import static com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Currency.Currency.DOLLARS;

public class RepairCurrencyConversionMapper extends ConfiguredMapper<Object, Text, NullWritable, Text> {

  public static final String COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  private RepairParser repairParser;
  private String columnSeparator;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
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

}
