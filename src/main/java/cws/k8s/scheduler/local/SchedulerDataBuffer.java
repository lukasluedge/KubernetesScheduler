package cws.k8s.scheduler.local;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class SchedulerDataBuffer {

    private static final List<Map<String, String>> buffer = new ArrayList<>();
    private static final Lock lock = new ReentrantLock();
    private static final Condition notEmpty = lock.newCondition();

    /**
     * Fügt ein neues Mapping hinzu (thread-sicher).
     * Weckt ggf. wartende Threads auf.
     */
    public void produce(Map<String, String> mapping) {

        lock.lock();
        try {
            buffer.add(new LinkedHashMap<>(mapping)); // Kopie, um Seiteneffekte zu vermeiden
            notEmpty.signalAll(); // Weckt wartende Consumer auf
        } finally {
            lock.unlock();
        }
    }

    /**
     * Liest alle bisher produzierten Mappings (blockiert non-busy,
     * bis mindestens eines verfügbar ist).
     * Gibt danach alle bis zu diesem Zeitpunkt vorhandenen Mappings zurück.
     */
    public static Map<String, String> consumeAllBlocking() throws InterruptedException {
        lock.lock();
        try {
            // warten, bis mindestens ein Mapping vorhanden ist
            while (buffer.isEmpty()) {
                notEmpty.await();
            }

            // Mappings zusammenführen
            Map<String, String> merged = new LinkedHashMap<>();
            for (Map<String, String> m : buffer) {
                if (m != null) {
                    merged.putAll(m); // spätere Einträge überschreiben frühere Keys
                }
            }

            buffer.clear(); // Buffer leeren nach dem Lesen
            return merged;
        } finally {
            lock.unlock();
        }
    }
}