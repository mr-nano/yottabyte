package com.thoughtworks.yottabyte.vehiclerepairdenormalization;

import com.thoughtworks.yottabyte.ConfiguredMapper;
import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.TaggedText;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.RecordType.REPAIR;

public class TaggedRepairMapper extends ConfiguredMapper<Object,Text,Text,TaggedText> {
  public static final String COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";

  @Override
  protected void map(Object key, Text row, Context context) throws IOException, InterruptedException {
    String columnSeparator = get(COLUMN_SEPARATOR);
    Repair repair = new Repair(new RepairData(row.toString(),columnSeparator));

    context.write(new Text(repair.getVehicleType()),
      new TaggedText<>(row, REPAIR));
  }

}

