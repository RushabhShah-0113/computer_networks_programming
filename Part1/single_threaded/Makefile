
PYTHON ?= python3

.PHONY: all clean

all: server client

server: server.py protocol.py
	printf '#!/usr/bin/env python3\nfrom server import main\nmain()\n' > server
	chmod +x server

client: client.py protocol.py metrics.py
	printf '#!/usr/bin/env python3\nfrom client import main\nmain()\n' > client
	chmod +x client

clean:
	rm -f server client
