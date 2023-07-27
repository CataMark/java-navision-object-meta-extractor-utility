package ro.any.cmutica.navmeta;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.zip.Inflater;

/**
 *
 * @author cmutica
 */
public class NavMeta {
    
    private static final String MAGIG_HEX = "02457d5b";
    private static final String SQL = """
                                    select a.[Object ID], a.[Object Type], a.Metadata, a.[User Code], a.[User AL Code], b.[Name] from dbo.[Object Metadata] as a
                                    left join dbo.[Object] as b
                                    on a.[Object ID] = b.[ID] and a.[Object Type] = b.[Type];                                       
                                    """;
    private static final String META_PATH = "C:/Users/cmutica.MOBILUX/Documents/NAV_2017/";
    
    private static String objTypeCaption(int objType) throws Exception{
        var result = switch (objType) {
            case 0:
                yield "TableData";
            case 1:
                yield "Table";
            case 2:
                yield "Form";
            case 3:
                yield "Report";
            case 5:
                yield "CodeUnit";
            case 6:
                yield "XMLPort";
            case 7:
                yield "MenuSuite";
            case 8:
                yield "Page";
            case 9:
                yield "Query";
            case 10:
                yield "System";
            case 11:
                yield "FieldNumber";
            default:
                throw new Exception("Not known object type");
        };
        return result;
    }
    
    private record ObjectMeta(Optional<String> name, Optional<Integer> objectType, Optional<Integer> objectId, Optional<byte[]> metadata, Optional<byte[]> userCode, Optional<byte[]> userAlCode) {};
    
    private static byte[] deflate(byte[] data) throws Exception{
        var magic_number = new BigInteger(MAGIG_HEX, 16).toByteArray();
        var decompresser = new Inflater(true);
        var dataLength = 0;
        if (Arrays.equals(magic_number, Arrays.copyOfRange(data, 0, 4))){
            dataLength = data.length - 4;
            decompresser.setInput(data, 4, dataLength);
        } else {
            dataLength = data.length;
            decompresser.setInput(data, 0, dataLength);
        }
        
        var buffer = new byte[1024];
        try(var oStream = new ByteArrayOutputStream(dataLength);){
            while (!decompresser.finished()){
                var count = decompresser.inflate(buffer);
                oStream.write(buffer, 0, count);
            }
            decompresser.end();
            oStream.close();
            return oStream.toByteArray();
        }
    }
    
    private static void writeToFile(ObjectMeta record) throws Exception{
        var path_text = String.format("%1$s%2$d_%3$s_%4$s",
                        META_PATH,
                        record.objectId.orElseThrow(() -> new Exception("No object ID from record!")),
                        record.name.orElse("").replace("/", "_"),
                        objTypeCaption(record.objectType.orElseThrow(() -> new Exception("No object type from record!")))
                    );
            
        if (record.metadata.isPresent()){
            var fPath = path_text + ".xml";
            try(var fWriter = new FileWriter(fPath, StandardCharsets.UTF_8, false);
                var bWriter = new BufferedWriter(fWriter);){
                bWriter.append(new String(deflate(record.metadata.get()), StandardCharsets.UTF_8));
                bWriter.flush();
                System.out.println("File created: " + fPath);
            }
        }

        if (record.userCode.isPresent()){
            var fPath = path_text + ".cs";
            try(var fWriter = new FileWriter(fPath, StandardCharsets.UTF_8, false);
                var bWriter = new BufferedWriter(fWriter);){
                bWriter.append(new String(deflate(record.userCode.get()), StandardCharsets.UTF_8));
                bWriter.flush();
                System.out.println("File created: " + fPath);
            }
        }

        if (record.userAlCode.isPresent()){
            var fPath = path_text + ".al";
            try(var fWriter = new FileWriter(fPath, StandardCharsets.UTF_8, false);
                var bWriter = new BufferedWriter(fWriter);){
                bWriter.append(new String(deflate(record.userAlCode.get()), StandardCharsets.UTF_8));
                bWriter.flush();
                System.out.println("File created: " + fPath);
            }
        }
    }
    
    public static void main(String[] args) {
        //creating database source
        var ds = new SQLServerDataSource();
        ds.setServerName("<removed>");
        ds.setPortNumber(1433);
        ds.setDatabaseName("any");
        ds.setEncrypt(false);
        ds.setTrustServerCertificate(true);
        ds.setAuthentication("SqlPassword");
        ds.setUser("<removed>");
        ds.setPassword("<removed>");
        
        //geting the record
        ObjectMeta record = null;
        try(var conn = ds.getConnection();
            var stmt = conn.prepareStatement(SQL);
            var rs = stmt.executeQuery()){
            
            while (rs.next()){
                record = new ObjectMeta(
                        Optional.ofNullable(rs.getString("Name")),
                        Optional.ofNullable(rs.getInt("Object Type")),
                        Optional.ofNullable(rs.getInt("Object ID")),
                        Optional.ofNullable(rs.getBytes("Metadata")),
                        Optional.ofNullable(rs.getBytes("User Code")),
                        Optional.ofNullable(rs.getBytes("User Al Code"))
                );
                writeToFile(record);
            }            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
