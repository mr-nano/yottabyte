package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.KEY_SEPARATOR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag.REPAIR;

public class TaggedRepairMapper extends Mapper<Object,Text,TaggedKey,Text> {
  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";

  private Configuration configuration;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
  }

  protected String get(String key){
    return Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
  }

  @Override
  protected void map(Object key, Text row, Context context) throws IOException, InterruptedException {
    String columnSeparator = get(REPAIR_COLUMN_SEPARATOR);
    Repair repair = new Repair(new RepairData(row.toString(),columnSeparator));

    context.write(new TaggedKey(repair.getVehicleType(),REPAIR),
      new Text(row + KEY_SEPARATOR + REPAIR));
  }

}

