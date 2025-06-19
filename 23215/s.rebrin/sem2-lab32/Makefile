CC = gcc
CFLAGS = -Wall -g
LDFLAGS = -pthread #-lsocket -lnsl
OBJECTS = proxy.o network.o cache.o thread.o
TARGET = proxy

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

proxy.o: proxy.c network.h cache.h
	$(CC) $(CFLAGS) -c proxy.c

network.o: network.c network.h
	$(CC) $(CFLAGS) -c network.c

cache.o: cache.c cache.h
	$(CC) $(CFLAGS) -c cache.c

thread.o: thread.c thread.h
	$(CC) $(CFLAGS) -c thread.c

.PHONY: all clean