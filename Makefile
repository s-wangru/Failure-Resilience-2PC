all: Commit_message.java Server.class UserNode.class

%.class: %.java
	javac $<

clean:
	rm -f *.class
