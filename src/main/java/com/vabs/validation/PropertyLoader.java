package com.vabs.validation;
import static com.nothing.utils.MarkdownConstants.prop;
import static com.nothing.utils.MarkdownConstants.fis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
public class PropertyLoader {
 public static Properties propLoad() throws Exception{
	 fis=new FileInputStream(new File(System.getProperty("user.dir")+"/MarkdownProperties.properties"));
	 prop.load(fis);
	
	 return prop;
  }
// public static void close() throws IOException{
//	 fis.close();
// }
}
