import com.sun.tools.internal.ws.wsdl.document.jaxws.Exception;

import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLAnalyzer {
    public Set keywords = new HashSet(){{
        add("alter");
        add("table");
        add("add");
        add("constraint");
        add("from");
        add("primary");
        add("key");
    }};

    public ArrayList<PKeyDepStru> tokenise(String sentence){
        sentence = sentence.trim();
        ArrayList<PKeyDepStru> pKeyDepStruLst = new ArrayList<>();
        String[] sentenceArray = sentence.split(";");
        for(String sen: sentenceArray){
            sen = replaceSymbol(sen);
            sen = replaceBlank(sen);
            sen = sen.toLowerCase();
            PKeyDepStru pKeyDepStru = tokenExtract(sen);
            pKeyDepStruLst.add(pKeyDepStru);
        }

        return  pKeyDepStruLst;
    }

    public String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s+|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll(" ");
        }
        return dest;
    }

    public String replaceSymbol(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\(|\\)|,|;");
            Matcher m = p.matcher(str);
            dest = m.replaceAll(" ");
        }
        return dest;
    }


    public PKeyDepStru tokenExtract(String sentence){
        PKeyDepStru pKeyDepStru;
        ArrayList pkeyList = new ArrayList();;
        String tableName = "";
        String constraintName = "";
        String [] arr = sentence.trim().split("\\s+");

        int i = 0;
        try {
            if (keywords.contains(arr[i])){
                if (arr[i].equals("alter")){
                    i ++;
                    if (keywords.contains(arr[i])){
                        if (arr[i].equals("table")){
                            i ++;
                            if (!keywords.contains(arr[i])){
                                tableName = arr[i];
                                i++;
                                if(arr[i].equals("add")){
                                    i ++;
                                    if(arr[i].equals("constraint")){
                                        i ++;
                                        if (!keywords.contains(arr[i])){
                                            constraintName = arr[i];
                                            i++;
                                            if(arr[i].equals("primary")){
                                                i++;
                                                if(arr[i].equals("key")){
                                                    i++;
                                                    while (!keywords.contains(arr[i])){
                                                        pkeyList.add(arr[i]);
                                                        i ++;
                                                        if(i >= arr.length) break;
                                                    }
                                                    if (pkeyList.size() == 0)
                                                        throw new java.lang.Exception();
                                                }else
                                                    throw new java.lang.Exception();
                                            }else
                                                throw new java.lang.Exception();
                                        } else
                                            throw new java.lang.Exception();
                                    }else
                                        throw new java.lang.Exception();
                                }else
                                    throw new java.lang.Exception();
                            } else
                                throw new java.lang.Exception();
                        }else
                            throw new java.lang.Exception();
                    }else
                        throw new java.lang.Exception();
                }else
                    throw new java.lang.Exception();
            }else
                throw new java.lang.Exception();
        } catch (java.lang.Exception e) {
            e.printStackTrace();
            System.err.println("SQL Wrong");
            return null;
        }

        return new PKeyDepStru(constraintName, tableName, pkeyList);
    }
}
