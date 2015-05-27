package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.thoughtworks.yottabyte.ConfiguredDriver;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.TaggedRepairMapper;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.TaggedVehicleMapper;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.TaggedText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.*;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce.RepairCurrencyConversionMapper.COLUMN_SEPARATOR;
import static org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath;

public class Driver extends ConfiguredDriver implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    loadPropertiesFile(args[0]);
    Configuration configuration = getConf();
    configuration.set(COLUMN_SEPARATOR, get(VEHICLES.columnSeparator()));

    Job job = Job.getInstance(configuration,this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    addInputPath(job,getPath(VEHICLES.path()), TextInputFormat.class,TaggedVehicleMapper.class);
    addInputPath(job,getPath(REPAIR_IN_DOLLARS.path()), TextInputFormat.class,TaggedRepairMapper.class);

    FileOutputFormat.setOutputPath(job, getPath(VEHICLES_REPAIRS.path()));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TaggedText.class);

    job.setReducerClass(DenormalizingReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

}
