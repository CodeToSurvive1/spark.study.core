package io.github.codetosurvive.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class SubStringUDF extends UDF {

	 public String evaluate(String a, int b, String c){
		 return a.toString();
	 }
	 
	 public String evaluate(String a){
		 return a.trim().toString();
	 }
}
