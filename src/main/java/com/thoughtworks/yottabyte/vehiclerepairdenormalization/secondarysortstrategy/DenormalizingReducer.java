package com.thoughtworks.yottabyte.vehiclerepairdenormalization.secondarysortstrategy;

import com.thoughtworks.yottabyte.ConfiguredReducer;
import com.thoughtworks.yottabyte.datamodels.RepairData;
import com.thoughtworks.yottabyte.datamodels.VehicleData;
import com.thoughtworks.yottabyte.repaircurrencyconversion.domain.Repair;
import com.thoughtworks.yottabyte.vehiclecount.domainmodels.Vehicle;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.RecordType;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.TaggedText;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.RecordType.REPAIR;
import static com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.RecordType.VEHICLE;

public class DenormalizingReducer extends ConfiguredReducer<Text, TaggedText<RecordType>, NullWritable, Text>{

  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";

  @Override
  protected void reduce(Text key, Iterable<TaggedText<RecordType>> rows, Context context) throws IOException, InterruptedException {

    List<Repair> repairs = new ArrayList<>();
    TaggedText<RecordType> taggedText = null;

    while (rows.iterator().hasNext()){
      taggedText = rows.iterator().next();
      if(taggedText.tag().isNot(REPAIR)){
        break;
      }
      repairs.add(new Repair(new RepairData(taggedText.row(),get(REPAIR_COLUMN_SEPARATOR))));
    }

    if(taggedText.tag().isNot(VEHICLE)){
      return;
    }

    Vehicle vehicle = new Vehicle(new VehicleData(taggedText.row(),get(VEHICLE_COLUMN_SEPARATOR), ""));

    for (Repair repair : repairs) {
      context.write(NullWritable.get(), new Text(vehicle.getRegistrationNumber() + "," + repair.getCode()));
    }

    for (TaggedText<RecordType> row : rows) {
      vehicle = new Vehicle(new VehicleData(row.row(),get(VEHICLE_COLUMN_SEPARATOR), ""));
      for (Repair repair : repairs) {
        context.write(NullWritable.get(), new Text(vehicle.getRegistrationNumber() + "," + repair.getCode()));
      }
    }

  }
}
