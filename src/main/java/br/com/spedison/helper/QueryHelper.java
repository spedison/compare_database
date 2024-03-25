package br.com.spedison.helper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public record QueryHelper (String database, String nameQuery){

    public String loadQuery(){
        return loadQuery(nameQuery);
    }

    public String loadQuery(String nameQueryAdd){
        String ret = "";

        try (InputStream is = this.getClass().getResourceAsStream("/br/com/spedison/queries/%s/%s.sql".formatted(database,nameQueryAdd))) {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            StringBuffer retBuf = new StringBuffer();
            while ((line = br.readLine()) != null)
            {
                retBuf.append(line);
                retBuf.append("%n".formatted());
            }
            br.close();
            isr.close();
            ret = retBuf.toString();
        } catch (IOException e) {
            return ret;
        }

        return ret;
    }
}
