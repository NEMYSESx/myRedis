package com.shakir.dis_api;
import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.*;

import org.springframework.stereotype.Service;

@Service
public class CacheService {
    private static final String LOG_FILE = "logs/cache_log.txt";
    private KeyValueLinkedList<String, Integer> linkedList;
    private ConsistentHashRing<String, Integer> hashRing;
    private ExecutorService executor;

    public CacheService() {
        this.linkedList = new KeyValueLinkedList<>(2);
        this.hashRing = new ConsistentHashRing<>(linkedList);
        this.executor = Executors.newSingleThreadExecutor();
        createLogDirectory();
        loadCacheFromLog();
    }

    public void put(String key, Integer value) {
        executor.submit(() -> {
            hashRing.addNode(key, value);
            logOperation("PUT", key, value);
        });
    }

    public Future<Integer> get(String key) {
        return executor.submit(() -> {
            KeyValueNode<String, Integer> node = hashRing.getNode(key);
            return (node != null) ? node.value : null;
        });
    }

    public void del(String key) {
        executor.submit(() -> {
            hashRing.removeNode(key);
            logOperation("DEL", key, null);
        });
    }

    private void logOperation(String operation, String key, Integer value) {
        try (PrintWriter out = new PrintWriter(new FileWriter(LOG_FILE, true))) {
            if (value != null) {
                out.println(operation + "," + key + "," + value);
            } else {
                out.println(operation + "," + key);
            }
        } catch (IOException e) {
            System.out.println("Error logging operation: " + e.getMessage());
        }
    }

    private void loadCacheFromLog() {
        try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3 && parts[0].equals("PUT")) {
                    String key = parts[1];
                    Integer value = Integer.parseInt(parts[2]);
                    hashRing.addNode(key, value);
                } else if (parts.length == 2 && parts[0].equals("DEL")) {
                    String key = parts[1];
                    hashRing.removeNode(key);
                }
            }
        } catch (IOException e) {
            System.out.println("Error loading cache from log: " + e.getMessage());
        }
    }

    private void createLogDirectory() {
        try {
            Files.createDirectories(Paths.get("logs"));
        } catch (IOException e) {
            System.out.println("Error creating log directory: " + e.getMessage());
        }
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    public class KeyValueNode<K, V> {
        K key;
        V value;
        KeyValueNode<K, V> prev;
        KeyValueNode<K, V> next;

        public KeyValueNode(K key, V value) {
            this.key = key;
            this.value = value;
            this.prev = null;
            this.next = null;
        }
    }

    public class KeyValueLinkedList<K, V> {
        private int capacity;
        private int size;
        private KeyValueNode<K, V> head;
        private KeyValueNode<K, V> tail;
        private HashMap<K, KeyValueNode<K, V>> nodeMap;

        public KeyValueLinkedList(int capacity) {
            this.capacity = capacity;
            this.size = 0;
            this.head = null;
            this.tail = null;
            this.nodeMap = new HashMap<>();
        }

        public void insertEnd(K key, V value) {
            KeyValueNode<K, V> newNode = new KeyValueNode<>(key, value);
            nodeMap.put(key, newNode);
            if (head == null) {
                head = newNode;
                tail = newNode;
            } else {
                tail.next = newNode;
                newNode.prev = tail;
                tail = newNode;
            }
            size++;
            if (size > capacity) {
                deleteStart();
            }
        }

        public void deleteStart() {
            if (head == null) {
                return;
            }
            nodeMap.remove(head.key);
            if (head == tail) {
                head = null;
                tail = null;
            } else {
                head = head.next; // Move head to the next node
                head.prev = null; // Remove the link to the old head
            }
            size--;
        }

        public void deleteNode(KeyValueNode<K, V> node) {
            if (node == null) return;

            nodeMap.remove(node.key);
            if (node.prev != null) {
                node.prev.next = node.next;
            } else {
                head = node.next;
            }

            if (node.next != null) {
                node.next.prev = node.prev;
            } else {
                tail = node.prev;
            }

            size--;
        }

        public void put(K key, V value) {
            if (nodeMap.containsKey(key)) {
                deleteNode(nodeMap.get(key));
            }
            insertEnd(key, value);
        }

        public V get(K key) {
            if (!nodeMap.containsKey(key)) {
                return null;
            }
            return nodeMap.get(key).value;
        }

        public void traverse() {
            KeyValueNode<K, V> current = head;
            while (current != null) {
                System.out.println("Key: " + current.key + ", Value: " + current.value);
                current = current.next;
            }
        }
    }

    // ConsistentHashRing class (unchanged)

    public class ConsistentHashRing<K, V> {
        private final int LIMIT = 100;
        private final long PRIME = 31;
        private final long ODD = 17;
        private TreeMap<Integer, KeyValueNode<K, V>> hashRing;
        private KeyValueLinkedList<K, V> linkedList;

        public ConsistentHashRing(KeyValueLinkedList<K, V> linkedList) {
            this.hashRing = new TreeMap<>();
            this.linkedList = linkedList;
        }

        private int calculateHash(K key) {
            return Math.abs(key.hashCode() % LIMIT);
        }

        public void addNode(K key, V value) {
            KeyValueNode<K, V> node = new KeyValueNode<>(key, value);
            int hash = calculateHash(key);
            if (!hashRing.containsKey(hash)) {
                hashRing.put(hash, node);
                linkedList.put(key, value);
            } else {
                while (hashRing.containsKey(hash)) {
                    hash = (hash + 1) % LIMIT;
                }
                hashRing.put(hash, node);
                linkedList.put(key, value);
            }
        }

        public KeyValueNode<K, V> getNode(K key) {
            if (hashRing.isEmpty()) {
                return null;
            }
            int hash = calculateHash(key);
            if (!hashRing.containsKey(hash)) {
                SortedMap<Integer, KeyValueNode<K, V>> tailMap = hashRing.tailMap(hash);
                hash = tailMap.isEmpty() ? hashRing.firstKey() : tailMap.firstKey();
            }
            return hashRing.get(hash);
        }

        public void removeNode(K key) {
            int hash = calculateHash(key);
            KeyValueNode<K, V> node = hashRing.remove(hash);
            if (node != null) {
                linkedList.deleteNode(node);
            }
        }

        public void traverseRing() {
            for (Map.Entry<Integer, KeyValueNode<K, V>> entry : hashRing.entrySet()) {
                System.out.println("Hash: " + entry.getKey() + ", Key: " + entry.getValue().key + ", Value: " + entry.getValue().value);
            }
        }
    }

}