package com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce;

import com.thoughtworks.yottabyte.ConfiguredDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DIFFERENT_CURRENCIES;
import static com.thoughtworks.yottabyte.constants.FileNameConstants.REPAIR_IN_DOLLARS;
import static com.thoughtworks.yottabyte.repaircurrencyconversion.mapreduce.RepairCurrencyConversionMapper.COLUMN_SEPARATOR;

public class RepairCurrencyConversionDriver extends ConfiguredDriver implements Tool{

  private static ClassLoader loader;

  @Override
  public int run(String[] args) throws Exception {

    loadPropertiesFile(args[0]);
    Configuration configuration = getConf();
    configuration.set(COLUMN_SEPARATOR, get(REPAIR_IN_DIFFERENT_CURRENCIES.columnSeparator()));

    Job job = Job.getInstance(configuration,this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    FileInputFormat.setInputPaths(job, getPath(REPAIR_IN_DIFFERENT_CURRENCIES.path()));
    FileOutputFormat.setOutputPath(job, getPath(REPAIR_IN_DOLLARS.path()));

    job.setMapperClass(RepairCurrencyConversionMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(0);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    loader = RepairCurrencyConversionDriver.class.getClassLoader();
    if(args.length < 1){
      args = new String[]{loader.getResource("properties.config").getPath()};
    }
    int exitCode = ToolRunner.run(new Configuration(), new RepairCurrencyConversionDriver(), args);
    System.exit(exitCode);
  }

}
