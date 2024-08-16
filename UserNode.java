
/**
 * UserNode.java
 * 
 * This class represents an individual User Node in a distributed collage-making system. The UserNode class is responsible
 * for participating in a two-phase commit protocol managed by a central server. It handles decision making for commits or
 * aborts based on user input and manages the local file state accordingly.
 * Each UserNode operates independently and communicates with the Server using a messaging protocol provided by ProjectLib.
 * It is initialized with unique identifiers and operates based on messages received through the network, making autonomous
 * decisions that are logged locally for reliability.
 * 
 * Functionalities:
 * 1. Participate in two-phase commits by responding to PREPARE and COMMIT messages from the Server.
 * 2. Maintain a local log file to record decisions and ensure recoverability in the event of a node failure.
 * 3. Lock and unlock image files that are under consideration in ongoing transactions to prevent conflicting operations.
 * 4. Respond to messages from the Server with vote commit or abort based on local checks and user decisions.
 * 5. Clean up local resources and state upon successful commit or abort, ensuring data consistency.
 * 
 * 
 * - This class utilizes synchronized lists and concurrent maps to manage state in a thread-safe manner, crucial for handling
 *   concurrent incoming messages and decisions.
 * - Recovery from crashes is supported by reading the log file at startup, which contains past decisions and states of file locks.
 * - User input is simulated through interactions, and actual user decision logic can be modified according to specific use case requirements.
 * 
 */

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.imageio.IIOException;

public class UserNode implements ProjectLib.MessageHandling {
    public final String myId;
    public static ProjectLib PL;
    public final String logFile;
    private FileWriter logHeader;

    private static List<String> locked_list = 
        Collections.synchronizedList(new ArrayList<>());

    /**
     * UserNode - Constructor initializes a User Node instance.
     * Sets up the logging mechanism and restores the node's state if 
     * a log file already exists.
     *
     * @param id The unique identifier for this User Node, used in logging 
     *           and as part of the node's address.
     * @throws Exception if an error occurs during setup or state restoration.
     */

    public UserNode(String id) throws Exception {
        myId = id;
        logFile = "log_" + id + ".txt";
        if (!new File(logFile).exists()) {
            new File(logFile).createNewFile();
        } else {
            restore();
            new FileOutputStream(logFile).close();
        }
        logHeader = new FileWriter(logFile);

    }

    /**
     * deserializeByteArrayToObject - Converts a byte array back into a 
     * Commit_message object.
     * This method is essential for interpreting the contents of messages 
     * received from the Server.
     *
     * @param bytes The byte array to be deserialized into a Commit_message 
     * object.
     * @return Commit_message The deserialized Commit_message object, or null 
     * if deserialization fails due to an error.
     */
    public static Commit_message deserializeByteArrayToObject(byte[] bytes) {
        ByteArrayInputStream byteArrayInputStream = 
                new ByteArrayInputStream(bytes);
        try (ObjectInputStream objectInputStream = 
                new ObjectInputStream(byteArrayInputStream)) {
            return (Commit_message) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Sends a reply message back to the server or another User Node.
     * Constructs a Commit_message from the given parameters and sends it to 
     * the specified address.
     *
     * @param request The original Commit_message that prompted this reply.
     * @param srcAdr The source address to which the reply should be sent.
     * @param message The type of message to send, typically a vote or 
     * acknowledgment.
     * @throws Exception if there is an error during message preparation or 
     * sending.
     */

    public static void replyBack(Commit_message request, String srcAdr, 
            String message) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        Commit_message m = new Commit_message(message, request.fileName, 
                request.content, request.sources);
        oos.writeObject(m);
        ProjectLib.Message newM = 
            new ProjectLib.Message(srcAdr, bos.toByteArray());
        PL.sendMessage(newM);
    }

    /**
     * writeToLog - Logs decisions made by the User Node to a local file.
     * This method supports recovery by logging all key decisions 
     * (commit, abort, etc.) that affect the node's state.
     *
     * @param decision The decision type 
     *                 (e.g., "Agree", "Reject", "COMMIT", "ABORT").
     * @param filename The filename associated with the decision.
     * @param sources An array of sources involved in the decision, 
     *                typically file names.
     * @throws IOException if there is an error writing to the log file.
     */

    public void writeToLog(String decision, String filename, String[] sources)
                            throws IOException {
        String s = decision + " " + filename + " ";
        for (String source : sources) {
            s += source + ",";
        }
        s = s.substring(0, s.length() - 1);
        s += "\n";
        logHeader.write(s);
        logHeader.flush();
    }

    /**
     * deliverMessage - Handles incoming messages from the Server or other 
     * User Nodes.
     * Processes messages based on their type (e.g., PREPARE, COMMIT_SUC) and 
     * takes appropriate action,
     * such as voting or executing commit/abort operations.
     *
     * @param msg The incoming ProjectLib.Message to be processed.
     * @return boolean True if the message was processed successfully, 
     *         false if an error occurred.
     */
    public boolean deliverMessage(ProjectLib.Message msg) {
        try {
            String srcAdr = msg.addr;
            Commit_message request = deserializeByteArrayToObject(msg.body);
            if (request.type.equals("PREPARE")) {
                for (String file : request.sources) {
                    if (!Files.exists(Paths.get(file)) 
                        || locked_list.contains(file)) {
                        replyBack(request, srcAdr, "VOTEABORT");
                        return true;
                    }
                }
                if (PL.askUser(request.content, request.sources)) {
                    for (String file : request.sources) {
                        locked_list.add(file);
                    }
                    replyBack(request, srcAdr, "VOTECOMMIT");
                    writeToLog("Agree", request.fileName, request.sources);
                    return true;
                } else {
                    replyBack(request, srcAdr, "VOTEABORT");
                    writeToLog("Reject", request.fileName, request.sources);
                    return true;
                }
            } else if (request.type.equals("COMMIT_SUC")) {
                writeToLog("COMMIT", request.fileName, request.sources);
                for (String file : request.sources) {
                    File f = new File(file);
                    f.delete();
                    locked_list.remove(file);
                }
                replyBack(request, srcAdr, "ACK");
                writeToLog("Finish", request.fileName, request.sources);
                return true;
            } else if (request.type.equals("COMMIT_FAIL")) {
                writeToLog("ABORT", request.fileName, request.sources);
                for (String file : request.sources) {
                    locked_list.remove(file);
                }
                replyBack(request, srcAdr, "ACK");
                writeToLog("Finish", request.fileName, request.sources);
                return true;
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * redoAction - Re-executes actions based on logged decisions during 
     * the recovery process.
     * Ensures that the node's state is consistent with decisions made 
     * before it failed.
     *
     * @param agreed Maps of filenames to source arrays where the node 
     * previously agreed to commit.
     * @param committed Maps of filenames to source arrays where commits 
     * were successfully completed.
     * @param aborted Maps of filenames to source arrays where transactions 
     * were aborted.
     * @throws Exception if an error occurs while re-executing actions.
     */
    private void redoAction(ConcurrentHashMap<String, String[]> agreed,
            ConcurrentHashMap<String, String[]> committed,
            ConcurrentHashMap<String, String[]> aborted) throws Exception {
        for (String s : agreed.keySet()) {
            String[] source = agreed.get(s);
            for (String sou : source) {
                locked_list.add(sou);
            }
        }
        for (String s : committed.keySet()) {
            String[] source = committed.get(s);
            for (String sou : source) {
                File f = new File(sou);
                f.delete();
                locked_list.remove(sou);
            }
            writeToLog("Finish", s, source);
        }
        for (String s : aborted.keySet()) {
            String[] source = aborted.get(s);
            for (String sou : source) {
                locked_list.remove(sou);
            }
            writeToLog("Finish", s, source);
        }
    }

    /**
     * restore - Restores the User Node's state from its log file upon restart.
     * This method parses the log to determine the last known state and actions 
     * of the node, which are then re-executed to bring the node back to a 
     * consistent state.
     *
     * @throws Exception if there is an error during restoration.
     */
    private void restore() throws Exception {
        FileReader tmp = new FileReader(logFile);
        BufferedReader log = new BufferedReader(tmp);
        String line;
        ConcurrentHashMap<String, String[]> agreed = 
            new ConcurrentHashMap<String, String[]>();
        ConcurrentHashMap<String, String[]> committed = 
            new ConcurrentHashMap<String, String[]>();
        ConcurrentHashMap<String, String[]> aborted = 
            new ConcurrentHashMap<String, String[]>();
        while ((line = log.readLine()) != null) {
            String[] info = line.split(" ");
            String action = info[0];
            String fileName = info[1];
            String[] sourceArray = info[2].split(",");
            if (action.equals("Agree")) {
                agreed.put(fileName, sourceArray);
            } else if (action.equals("COMMIT")) {
                agreed.remove(fileName);
                committed.put(fileName, sourceArray);
            } else if (action.equals("ABORT")) {
                agreed.remove(fileName);
                aborted.put(fileName, sourceArray);
            } else if (action.equals("Finish")) {
                agreed.remove(fileName);
                committed.remove(fileName);
                aborted.remove(fileName);
            }
        }
        redoAction(agreed, committed, aborted);
    }

    /**
     * main - Entry point for the UserNode application.
     * Initializes a UserNode instance with the specified port and identifier, 
     * setting up its communication and operational parameters.
     *
     * @param args Command line arguments providing the port and identifier 
     * needed to start the node.
     * @throws Exception if the necessary arguments are not provided or if 
     * there is an error during initialization.
     */
    public static void main(String args[]) throws Exception {
        if (args.length != 2)
            throw new Exception("Need 2 args: <port> <id>");

        UserNode UN = new UserNode(args[1]);
        PL = new ProjectLib(Integer.parseInt(args[0]), args[1], UN);

        ProjectLib.Message msg = 
                new ProjectLib.Message("Server", "hello".getBytes());
        System.out.println(args[1] + ": Sending message to " + msg.addr);

    }
}
