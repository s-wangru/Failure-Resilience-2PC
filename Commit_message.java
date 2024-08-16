/**
 * Commit_message.java
 * 
 * Overview:
 * This class defines the structure of commit messages used in a distributed 
 * collage-making system.
 * It implements the Serializable interface to allow commit messages to be sent 
 * over the network
 * between the Server and User Nodes as part of a two-phase commit protocol.
 * 
 * */


import java.io.Serializable;

public class Commit_message implements Serializable {

    //type of message
    public String type;

    //filename of the collage
    public String fileName;

    //content of the collage
    public byte[] content;

    //source files for the collage
    public String[] sources;

    public Commit_message(String type, String fileName, 
                            byte[] content, String[] sources) {
        this.type = type;
        this.fileName = fileName;
        this.content = content;
        this.sources = sources;
    }

}