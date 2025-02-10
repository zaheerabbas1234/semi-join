package com.semijoin;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SemiJoinReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String employeeData = null;
        String departmentData = null;
        
        // Iterate over all values for this key (EmployeeID)
        for (Text val : values) {
            String[] fields = val.toString().split(",");
            if (fields[0].equals("A")) {
                employeeData = fields[1];  // Employee Name from Dataset A
            } else if (fields[0].equals("B")) {
                departmentData = fields[1];  // Department from Dataset B
            }
        }
        
        // If both employee and department data are found, emit the employee data from Dataset A
        if (employeeData != null && departmentData != null) {
            context.write(key, new Text(employeeData));  // Output: EmployeeID -> Employee Name
        }
    }
}
 
