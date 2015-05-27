package com.thoughtworks.yottabyte.vehiclecount;

import com.thoughtworks.yottabyte.ConfiguredDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.OLD_VEHICLES;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.VEHICLES;
import static com.thoughtworks.yottabyte.vehiclecount.OlderVehicleMapper.COLUMN_SEPARATOR;

public class OldVehicleCountDriver extends ConfiguredDriver implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    loadPropertiesFile(args[0]);
    Configuration configuration = getConf();
    configuration.set(COLUMN_SEPARATOR, get(VEHICLES.columnSeparator()));

    Job job = Job.getInstance(configuration,this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    FileInputFormat.setInputPaths(job, getPath(VEHICLES.path()));
    FileOutputFormat.setOutputPath(job, getPath(OLD_VEHICLES.path()));

    job.setMapperClass(OlderVehicleMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(VehicleCountReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

}
