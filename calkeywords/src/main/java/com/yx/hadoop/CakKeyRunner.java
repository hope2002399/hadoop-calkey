package com.iris.egrant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 解決思路：
 * 1,将每个项目所有的关键词全部排序后写入文件（重新写回数据库亦可以）
 * 2,读取文件，采用5,4,3,2组合的方式利用hadoop进行计算 （mapper阶段）
 * 3,回写数据库 将相应的结果写回数据库即可 (出现oom)
 * 3.1改成写文件然后通过load data 数据导入数据库
 *
 * @author Administrator
 */
public class CakKeyRunner {


    static String URL = "jdbc:oracle:thin:@192.168.15.8:1521:ora11g";
    static String USERNAME = "inno_bdsp_dev";
    static String PASSWORD = "inno_bdsp_dev";
    static String FILEPATH = "e://tmp/hadoop.txt";
    static String FILEPATH2 = "e://tmp/hadoop";

    static File f = new File(FILEPATH);
    static FileOutputStream fos = null;

    public static void main(String[] args) throws Exception {
        String hadoopDir = "";
        String os = System.getProperty("os.name");
        if (os.startsWith("Win") || os.startsWith("win")) {
            // 本地环境运行 hadoop 设置
            System.setProperty("hadoop.home.dir", "D:\\iriswork\\tools\\hadoop-2.7.6");
            hadoopDir = "D:/jobfiles";
            FILEPATH = "e://tmp/hadoop.txt";

        } else {
            FILEPATH = "/tmp/calkey.txt";
            hadoopDir = "/tmp/datastat";
        }
        Configuration conf = new Configuration();

        conf.set("mapred.child.java.opts", "-Xmx512m");
        conf.setInt("mapreduce.task.io.sort.mb", 256);
        conf.setInt("mapreduce.job.maps", 100);

        //TODO 必须 写在创建之后
        DBConfiguration.configureDB(conf, "oracle.jdbc.driver.OracleDriver", URL, USERNAME, PASSWORD);

        Job job = Job.getInstance(conf, "CakKeyRunner");
// 获取
        DBConfiguration dbConfiguration = new DBConfiguration(conf);
        //1. 先读取数据写入文件
        Connection connection = dbConfiguration.getConnection();
        Statement st = connection.createStatement();
        String querySql = "select prj.feature_words from  tmp_prj_classify t left join  prj_feature   prj  on prj.prp_code = t.prp_code    ";
//        String querySql = "select prj.feature_words from  tmp_prj_classify t left join  prj_feature   prj  on prj.prp_code = t.prp_code where  t.prp_code =100022305";
        ResultSet rs = st.executeQuery(querySql);
        fos = new FileOutputStream(f);
        while (rs.next()) {
            String feature_words = rs.getString("feature_words");
            feature_words = feature_words + "\n";
            fos.write(feature_words.getBytes(), 0, feature_words.getBytes().length);
        }
        fos.flush();
        fos.close();
        System.out.println("=====>插入文件 ==>finished<=========");

        // 2.hadoop 进行处理   5，4，3，2
        // 设置运行的jar
        job.setJarByClass(CakKeyRunner.class);
        // 设置运行的mapper
        job.setMapperClass(KeyCalMapper.class);
        // 设置运行的reduce
        job.setReducerClass(KeyCalReduce.class);

        //设置map阶段 输出key类型
        job.setMapOutputKeyClass(Text.class);
        // 设置map阶段 value 的数据类型
        job.setMapOutputValueClass(IntWritable.class);

        // 设置整个输出阶段 key 类型
//        job.setOutputKeyClass(KeyCotfVO.class);
        job.setOutputKeyClass(Text.class);
        //设置reduce阶段输出value的 类型
        job.setOutputValueClass(NullWritable.class);
        // 设输出源类型 db处理方式
//        job.setOutputFormatClass(DBOutputFormat.class);
        //设置表名和字段
        String[] fieldNames = new String[]{"key_combine", "key_combine_size", "cotf"};
        // 设置输入源
        FileInputFormat.setInputPaths(job, new Path(FILEPATH));
        FileOutputFormat.setOutputPath(job, new Path(FILEPATH2));
//        FileOutputFormat.setOutputPath(job, new Path(FILEPATH2,  ""));
//        DBOutputFormat.setOutput(job, "TMP_KEY_COTF", fieldNames);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        System.out.println("=====>finished<=========");
    }

}
