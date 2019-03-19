import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

/**
 * @author yxstart
 * @create 2019/3/15,10:22
 */
public class insertDataByFileTest {

    private static final String SPLIT = "##@@##";
    public static void main(String[] args) {
        String line = "020;电子商务;管理平台;农产品;农村电子商务;项目;农产品电子商务;研究;信息化管理;信息管理" ;  // k1;k2;k3;k4;k5..

        String[] keySplit = line.split(";");
        Arrays.sort(keySplit);
        int keySize = keySplit.length;
        String outKey ="";
        for (int i = 0; i <keySize; i++) {
            for (int j = i+1; j < keySize; j++) {
                outKey = combine(keySplit[i],keySplit[j]);
                outKey= outKey + SPLIT +"2";
                System.out.println(outKey);
                 for (int k = j+1; k < keySize; k++) {
                    outKey = combine(keySplit[i],keySplit[j],keySplit[k]);
                    outKey= outKey + SPLIT +"3";
                     System.out.println(outKey);
                     for (int m = k+1; m < keySize; m++) {
                        outKey = combine(keySplit[i],keySplit[j],keySplit[k],keySplit[m]);
                        outKey= outKey + SPLIT +"4";
                         System.out.println(outKey);
                         for (int n = m+1; n < keySize; n++) {
                            outKey = combine(keySplit[i],keySplit[j],keySplit[k],keySplit[m],keySplit[n]);
                            outKey= outKey + SPLIT +"5";
                             System.out.println(outKey);
                         }
                    }
                }
            }
        }
    }

    private static String combine(String...str){
        StringBuilder sb = new StringBuilder();
        for(String s:str){
            sb.append(";"+s) ;
        }
        return sb.toString().substring(1) ;
    }
}
