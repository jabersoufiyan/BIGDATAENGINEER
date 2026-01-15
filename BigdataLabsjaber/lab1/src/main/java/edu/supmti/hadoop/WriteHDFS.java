package edu.supmti.hadoop;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: hadoop jar WriteHDFS.jar <filepath> <content>");
            System.exit(1);
        }
        
        String filePathStr = args[0];
        String content = args[1];
        
        Configuration conf = new Configuration();
        FileSystem fs = null;
        
        try {
            fs = FileSystem.get(conf);
            Path filePath = new Path(filePathStr);
            
            // Créer les dossiers parents si nécessaire
            if (!fs.exists(filePath.getParent())) {
                fs.mkdirs(filePath.getParent());
            }
            
            // Créer le fichier et écrire le contenu
            FSDataOutputStream outStream = fs.create(filePath);
            outStream.writeUTF("Bonjour tout le monde!\n");
            outStream.writeUTF(content + "\n");
            outStream.close();
            
            System.out.println("File written successfully to " + filePathStr);
            
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }
}