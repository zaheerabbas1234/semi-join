package com.semijoin;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SemiJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");  // Split by commas
        
        // If the record is from Dataset A (employee record)
        if (fields[0].equals("A")) {
            String employeeID = fields[1];
            String name = fields[2];
            context.write(new Text(employeeID), new Text("A," + name));  // Emit key-value pair: EmployeeID -> ("A", Name)
        } 
        // If the record is from Dataset B (department record)
        else if (fields[0].equals("B")) {
            String employeeID = fields[1];
            String department = fields[2];
            context.write(new Text(employeeID), new Text("B," + department));  // Emit key-value pair: EmployeeID -> ("B", Department)
        }
    }
}
