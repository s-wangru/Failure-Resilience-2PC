
/**
 * Server.java
 * 
 * Overview:
 * This Server class implements the coordination logic for a distributed collage-making system,
 * utilizing a two-phase commit protocol to ensure consistency and atomicity of transactions.
 * The class manages the initiation, preparation, commit, and abort processes for collage 
 * submissions received from multiple User Nodes.
 * The Server is launched by specifying a network port which it uses to communicate with User Nodes.
 * It receives messages encapsulating user decisions and coordinates the commit or abort actions based
 * on user responses and system state.
 * 
 * Functionalities:
 * 1. Coordinate the two-phase commit protocol to process collage image submissions.
 * 2. Handle lost messages and node failures using retransmission and recovery strategies.
 * 3. Manage concurrent transactions ensuring isolation and durability of commit operations.
 * 4. Maintain a log file for recovery purposes, supporting system continuity across restarts.
 * 
 * - The server uses non-blocking I/O and concurrent data structures to handle multiple incoming commit
 *   requests efficiently.
 * - Failure handling is integrated to manage partial failures during the commit phases and to ensure the
 *   system can recover to a consistent state after restarts.
 * - Logging mechanisms are employed to support crash recovery, with detailed transaction states logged
 *   to enable precise rollback or commit on recovery.
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
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class Server implements ProjectLib.CommitServing {

    private static final ConcurrentHashMap<String, Commit_Thread> commitThreads
                         = new ConcurrentHashMap<>();

    public static ProjectLib PL;

    private static final String logFile = "log";

    private static FileWriter logHeader;
    private static Integer nextID = 3;

    //(1 for new collage, 2 for re-commit, 3 for re-abort).
    private final static int NEWC = 1;
    private final static int COMMIT = 2;
    private final static int ABORT = 3;

    // the timeout period to indicate a message is lost
    private final static int TIMEOUT_THRESHOLD = 3000;

    /**
     * Synchronizes and increments the global transaction ID counter.
     * This method ensures unique transaction IDs are generated for each new 
     * commit process.
     * 
     * @return the next transaction ID as an integer.
     */
    private synchronized int updateID() {
        nextID += 1;
        return nextID;
    }

    /**
     * Writes transaction decisions and details to the server log file.
     * This method is crucial for recovery processes, ensuring that all 
     * transaction states are recorded persistently.
     * 
     * @param decision The decision made (prepare, commit, abort, etc.).
     * @param ID       The unique transaction ID.
     * @param filename The filename associated with the transaction.
     * @param sources  The sources involved in the transaction.
     * @throws IOException if there is an error writing to the log file.
     */
    public void writeToLog(String decision, int ID, String filename,
            String sources) throws IOException {
        String s = Integer.toString(ID) + "@ " + decision + "@ "
                + filename + "@ " + sources + "\n";
        logHeader.write(s);
        logHeader.flush();
        PL.fsync();
    }

    /**
     * Commit_Thread - Represents a thread handling a specific commit process.
     * This inner class manages the lifecycle of a collage commit, from 
     * preparation to final acknowledgment.
     * It handles both commit and abort scenarios based on the responses from 
     * User Nodes.
     * 
     */

    private class Commit_Thread implements Runnable {

        // Array of strings identifying the source images and their 
        //respective User Nodes.
        private String[] sources;

        // Byte array containing the image data of the collage.
        private byte[] img;

        // The name of the file being committed or aborted.
        private String filename;

        // Integer indicating the type of operation 
        private int type;

        // A concurrent hash map storing User Nodes and their respective
        // image files involved in the transaction.
        private ConcurrentHashMap<String, ArrayList<String>> sourceFile 
                = new ConcurrentHashMap<>();

        // A blocking queue for messages related to this commit thread.
        private BlockingQueue<ProjectLib.Message> msgQueue 
                = new LinkedBlockingQueue<>();

        /**
         * Commit_Thread Constructor - Initializes a new commit operation.
         * Sets up the initial state for handling a commit or abort based on 
         * the provided parameters.
         * 
         * @param fileName The name of the file associated with the commit.
         * @param img      The image data to be committed.
         * @param sources  Array of sources participating in this commit.
         * @param type     The type of commit operation (1 for commit, 
         *                 2 for re-commit,
         *                 3 for abort).
         */

        private Commit_Thread(String fileName, byte[] img, 
                                String[] sources, int type) {
            this.filename = fileName;
            this.img = img;
            this.sources = sources;
            this.type = type;
        }

        private Commit_Thread(String fileName, ConcurrentHashMap<String, 
                                ArrayList<String>> sourceFile, int type) {
            filename = fileName;
            img = new byte[0];
            this.sourceFile = sourceFile;
            this.type = type;
        }

        /**
         * run - The main execution method for the commit thread.
         * Orchestrates the commit process, including sending prepare requests,
         * collecting votes,
         * and finalizing the commit or abort based on responses.
         */
        @Override
        public void run() {
            try {
                commitThreads.put(filename, this);
                int transID = updateID();
                if (type == COMMIT) {
                    commit(transID);
                    return;
                } else if (type == ABORT) {
                    abort(transID);
                    return;
                }
                buildSourceMap();
                prepare();
                writeToLog("prepare",transID, filename, sourceFile.toString());

                int numUser = sourceFile.size();
                long start = System.currentTimeMillis();
                boolean agreed = true;

                while (numUser > 0 && agreed &&
                        System.currentTimeMillis() - start<TIMEOUT_THRESHOLD){
                    while (msgQueue.size() > 0) {
                        ProjectLib.Message msg = msgQueue.poll();
                        if (sourceFile.keySet().contains(msg.addr)) {
                            Commit_message reply = deserialize(msg.body);
                            if (reply.type.equals("VOTECOMMIT")) {
                                numUser--;
                            } else {
                                agreed = false;
                                break;
                            }
                        }
                    }
                }
                if (numUser == 0 && agreed) {
                    commit(transID);
                } else {
                    abort(transID);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * abort - Handles the abort operation for a transaction.
         * Logs the abort decision, informs all User Nodes involved, and handle
         * cleanup.
         * 
         * @param transID The transaction ID of the current operation.
         * @throws Exception if there is an error during the abort process.
         */

        public void abort(int transID) throws Exception {
            writeToLog("abort", transID, filename, sourceFile.toString());
            String result = "";
            for (String key : sourceFile.keySet()) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                ArrayList<String> localSrc = sourceFile.get(key);
                String[] srcArr = new String[localSrc.size()];
                result = "COMMIT_FAIL";
                Commit_message m = new Commit_message("COMMIT_FAIL", filename,
                        img, localSrc.toArray(srcArr));
                oos.writeObject(m);
                ProjectLib.Message newM = new ProjectLib.Message(key, 
                                            bos.toByteArray());
                PL.sendMessage(newM);

            }
            receiveAck(transID, result);
        }

        /**
         * prepare - Sends a prepare message to all User Nodes involved in the
         * transaction.
         * Initiates the voting process by sending the current state and 
         * awaiting responses.
         * 
         * @throws Exception if there is an error during message preparation or 
         * sending.
         */

        public void prepare() throws Exception {
            for (String key : sourceFile.keySet()) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                ArrayList<String> localSrc = sourceFile.get(key);
                String[] srcArr = new String[localSrc.size()];
                Commit_message m = new Commit_message("PREPARE", filename, 
                                    img, localSrc.toArray(srcArr));
                oos.writeObject(m);
                ProjectLib.Message newM = new ProjectLib.Message(key, 
                                            bos.toByteArray());
                PL.sendMessage(newM);
            }
        }

        /**
         * buildSourceMap - Constructs a mapping of User Nodes to their 
         * respective source images.
         * This map is used to track which User Nodes have approved the 
         * commit and manage the image files.
         */
        public void buildSourceMap() {
            for (String s : sources) {
                String[] splitted = s.split(":");
                String srcadr = splitted[0];
                ArrayList<String> files = sourceFile.get(srcadr);
                if (files == null) {
                    files = new ArrayList<>();
                    files.add(splitted[1]);
                    sourceFile.put(srcadr, files);
                } else {
                    files.add(splitted[1]);
                }
            }
        }

        /**
         * commit - Handles the commit operation for a transaction.
         * Logs the commit decision, informs all User Nodes involved
         * 
         * @param transID The transaction ID for this commit.
         * @throws Exception if there is an error during the commit operation
         */

        public void commit(int transID) throws Exception {
            if (type == NEWC) {
                FileOutputStream out = new FileOutputStream(filename);
                out.write(img);
                out.close();
                writeToLog("commit", transID, filename, sourceFile.toString());
            }
            String result = "";
            for (String key : sourceFile.keySet()) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                ArrayList<String> localSrc = sourceFile.get(key);
                String[] srcArr = new String[localSrc.size()];
                result = "COMMIT_SUC";
                Commit_message m = new Commit_message("COMMIT_SUC", filename, 
                        img,localSrc.toArray(srcArr));
                oos.writeObject(m);
                ProjectLib.Message newM = new ProjectLib.Message(key, 
                        bos.toByteArray());
                PL.sendMessage(newM);
            }

            receiveAck(transID, result);
        }

        /**
         * receiveAck - Awaits acknowledgments from all User Nodes after 
         * a commit or abort notification.
         * Ensures that all User Nodes have processed the final decision
         * 
         * @param transID The transaction ID associated with the commit 
         *                or abort.
         * @param result  The result of the commit (e.g., "COMMIT_SUC" or
         *                "COMMIT_FAIL").
         * @throws Exception if there is an error in receiving acknowledgments
         *                  or during retransmission.
         */
        public void receiveAck(int transID, String result) throws Exception {
            long ackStart = System.currentTimeMillis();
            while (sourceFile.size() > 0) {
                if (type != NEWC) {
                    ProjectLib.Message msg;
                    while ((msg = PL.getMessage()) != null) {
                        Commit_message m = deserialize(msg.body);
                        if (commitThreads.get(m.fileName) != null) {
                            commitThreads.get(m.fileName).msgQueue.put(msg);
                        }
                    }
                }
                while (msgQueue.size() > 0) {
                    ProjectLib.Message msg = msgQueue.poll();
                    if (sourceFile.keySet().contains(msg.addr)) {
                        Commit_message reply = deserialize(msg.body);
                        if (reply.type.equals("ACK")) {
                            sourceFile.remove(msg.addr);
                        }
                    }
                }
                if (System.currentTimeMillis() - ackStart>=TIMEOUT_THRESHOLD){
                    for (String key : sourceFile.keySet()) {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        ArrayList<String> localSrc = sourceFile.get(key);
                        String[] srcArr = new String[localSrc.size()];
                        Commit_message m = new Commit_message(result, filename, 
                                img,localSrc.toArray(srcArr));
                        oos.writeObject(m);
                        ProjectLib.Message newM = new ProjectLib.Message(key,
                                bos.toByteArray());
                        PL.sendMessage(newM);
                    }
                    ackStart = System.currentTimeMillis();
                }

            }
            writeToLog("finished", transID, filename, sourceFile.toString());
            commitThreads.remove(filename);
        }

        /**
         * insertMessage - Inserts a message into the commit thread's 
         * message queue.
         * Used for handling responses from User Nodes asynchronously.
         * 
         * @param msg The message received from a User Node.
         */
        public void insertMessage(ProjectLib.Message msg) {
            msgQueue.offer(msg);
        }

    }

    /**
     * startCommit - Initiates a new commit thread for handling a collage
     * submission.
     * This method is called when a new collage request is received. It starts 
     * the two-phase commit process by creating a new Commit_Thread 
     * instance and running it.
     * 
     * @param filename The name of the file being committed.
     * @param img      The image data of the file.
     * @param sources  An array of source identifiers contributing to the file.
     */
    public void startCommit(String filename, byte[] img, String[] sources) {
        try {
            System.out.println("Server: Got request to commit " + filename);
            Commit_Thread m = new Commit_Thread(filename, img, sources, NEWC);
            Thread t = new Thread(m);
            t.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * deserialize - Converts a byte array back into a Commit_message object.
     * This method is essential for interpreting the contents of messages 
     * received from User Nodes.
     * It deserializes the byte array payload of incoming messages into
     * Commit_message objects, enabling the server to process and respond 
     * to commit requests and replies.
     * 
     * @param bytes The byte array to be deserialized into a Commit_message 
     *              object.
     * @return Commit_message The deserialized Commit_message object, or 
     *         null if deserialization fails.
     */
    public static Commit_message deserialize(byte[] bytes) {
        ByteArrayInputStream byteArrayInputStream 
                = new ByteArrayInputStream(bytes);
        try (ObjectInputStream objectInputStream 
                = new ObjectInputStream(byteArrayInputStream)) {
            return (Commit_message) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * reconstructMap - Reconstructs a concurrent hash map from a string
     * representation.
     * This utility function parses a serialized map string back into a 
     * concurrent hash map.
     * It is primarily used during the recovery process to restore the state of
     * source file mappings
     * from their logged string representations.
     * 
     * @param mapString The string representation of the map, typically 
     *                  retrieved from a log file.
     * @return ConcurrentHashMap<String, ArrayList<String>> A reconstructed 
     *         map of UserNode IDs to their list of files.
     */
    public static ConcurrentHashMap<String, ArrayList<String>> reconstructMap
                                                            (String mapString){
        ConcurrentHashMap<String, ArrayList<String>> map 
                                            = new ConcurrentHashMap<>();

        String trimmed = mapString.substring(1, mapString.length() - 2);
        String[] entries = trimmed.split("\\], ");
        for (String entry : entries) {
            String[] keyV = entry.split("=\\[");
            String[] values = keyV[1].split(", ");
            ArrayList<String> files = new ArrayList<>();
            for (String v : values) {
                files.add(v);
            }
            map.put(keyV[0], files);

        }

        return map;
    }

    /**
     * redoAction - Re-executes commit or abort actions during server recovery.
     * This method is called during the recovery process to redo any commit or 
     * abort actions that were underway before the server crashed. It ensures
     * that all transactions reach a consistent state based on the last known 
     * actions logged before the crash.
     * 
     * @param abort         A concurrent hash map containing transaction IDs 
     *                      and their respective states that need to be aborted.
     * @param abortThreads  A concurrent hash map storing threads that will
     *                      handle the abort actions for each transaction.
     * @param commit        A concurrent hash map containing transaction IDs &
     *                      their respective states that need to be committed.
     * @param commitThreads A concurrent hash map storing threads that will 
     *                      handle the commit actions for each transaction.
     */
    public void redoAction(ConcurrentHashMap<Integer, String[]> abort,
            ConcurrentHashMap<Integer, Commit_Thread> abortThreads,
            ConcurrentHashMap<Integer, String[]> commit,
            ConcurrentHashMap<Integer, Commit_Thread> commitThreads) {
        for (int ID : commit.keySet()) {
            String[] info = commit.get(ID);
            String filename = info[0];
            String source = info[1];
            ConcurrentHashMap<String, ArrayList<String>> sourceMap = 
                                                reconstructMap(source);
            Commit_Thread m = new Commit_Thread(filename, sourceMap, COMMIT);
            Thread t = new Thread(m);
            commitThreads.put(ID, m);
        }
        for (Integer ID : commitThreads.keySet()) {
            commitThreads.get(ID).run();
        }
        for (int ID : abort.keySet()) {
            String[] info = abort.get(ID);
            String filename = info[0];
            String source = info[1];
            ConcurrentHashMap<String, ArrayList<String>> sourceMap = 
                                                reconstructMap(source);
            Commit_Thread m = new Commit_Thread(filename, sourceMap, ABORT);
            Thread t = new Thread(m);
            abortThreads.put(ID, m);
        }
        for (Integer ID : abortThreads.keySet()) {
            abortThreads.get(ID).run();
        }

    }

    /**
     * restore - Restores the server state from the log file upon restart.
     * This method reads the log file entries to rebuild the active transaction
     * states and resumes them appropriately. It is key to recovery after a 
     * crash.
     * 
     * @throws Exception if there is an error during the restoration process.
     */
    public void restore() throws Exception {
        FileReader tmp = new FileReader(logFile);
        BufferedReader log = new BufferedReader(tmp);
        String line;
        ConcurrentHashMap<Integer, String[]> abort 
                                = new ConcurrentHashMap<Integer, String[]>();
        ConcurrentHashMap<Integer, Commit_Thread> abortThreads 
                                = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, String[]> commit 
                                = new ConcurrentHashMap<Integer, String[]>();
        ConcurrentHashMap<Integer, Commit_Thread> commitThreads 
                                = new ConcurrentHashMap<>();
        while ((line = log.readLine()) != null) {
            String[] info = line.split("@ ");
            int ID = Integer.parseInt(info[0]);
            String action = info[1];
            if (action.equals("prepare")) {
                String[] restoreInfo = new String[2];
                System.arraycopy(info, 2, restoreInfo, 0, 2);
                abort.put(ID, restoreInfo);
            } else if (action.equals("commit")) {
                String[] restoreInfo = new String[2];
                System.arraycopy(info, 2, restoreInfo, 0, 2);
                abort.remove(ID);
                commit.put(ID, restoreInfo);
            } else if (action.equals("abort")) {
                if (abort.get(ID) == null) {
                    String[] restoreInfo = new String[2];
                    System.arraycopy(info, 2, restoreInfo, 0, 2);
                    abort.put(ID, restoreInfo);
                }
            } else {
                commit.remove(ID);
                abort.remove(ID);
            }
        }
        redoAction(abort, abortThreads, commit, commitThreads);
    }

    /**
     * main - Entry point for the Server program.
     * This method sets up the server, initializes the communication library,
     * and enters a loop to handle incoming messages continuously.
     * 
     * @param args Command line arguments containing the server's network port.
     * @throws Exception if there is an error in setting up the server.
     */
    public static void main(String args[]) throws Exception {
        if (args.length != 1)
            throw new Exception("Need 1 arg: <port>");
        Server srv = new Server();
        PL = new ProjectLib(Integer.parseInt(args[0]), srv);
        File f = new File(logFile);
        if (!f.exists()) {
            f.createNewFile();
        } else {
            logHeader = new FileWriter(logFile, true);
            srv.restore();
            new FileOutputStream(logFile).close();
        }
        logHeader = new FileWriter(logFile);

        // main loop
        while (true) {
            ProjectLib.Message msg = PL.getMessage();
            Commit_message m = deserialize(msg.body);
            commitThreads.get(m.fileName).msgQueue.put(msg);
        }
    }
}
