package com.sarbesh.opentext;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class Main {

    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }

    /**
     * Implementation of the TaskExecutor interface that handles task execution with
     * concurrency control and task group management.
     */
    public static class TaskExecutorImpl implements TaskExecutor {
        
        /**
         * Executor service to handle concurrent task execution.
         * Uses a fixed thread pool with configurable size.
         */
        private final ExecutorService executorService;
        
        /**
         * Queue to maintain the order of tasks as they are submitted.
         * Uses BlockingQueue to ensure thread-safe operations.
         */
        private final BlockingQueue<Task<?>> taskQueue;
        
        /**
         * Map to manage concurrency control for task groups.
         * Each task group gets its own Semaphore to prevent concurrent execution.
         */
        private final Map<TaskGroup, Semaphore> groupLocks;

        /**
         * Constructor that initializes the executor service with a fixed thread pool.
         *
         * @param maxConcurrency Maximum number of concurrent tasks that can run.
         *                      If null or non-positive, a cached thread pool is used instead.
         */
        public TaskExecutorImpl(Integer maxConcurrency) {
            // Create a fixed thread pool with specified concurrency limit
            if (maxConcurrency == null || maxConcurrency <= 0) {
                this.executorService = Executors.newCachedThreadPool();
            } else {
                this.executorService = Executors.newFixedThreadPool(maxConcurrency);
            }
            // Initialize the task queue
            this.taskQueue = new LinkedBlockingQueue<>();
            // Initialize the map for task group locks
            this.groupLocks = new ConcurrentHashMap<>();
            // Start the task processor
//            startTaskProcessor();
        }

        /**
         * Constructor that initializes the executor service with a cached thread pool.
         * This allows dynamic thread creation as needed.
         */
        public TaskExecutorImpl() {
            // Create a fixed thread pool with specified concurrency limit
            this.executorService = Executors.newCachedThreadPool();
            // Initialize the task queue
            this.taskQueue = new LinkedBlockingQueue<>();
            // Initialize the map for task group locks
            this.groupLocks = new ConcurrentHashMap<>();
            // Start the task processor
//            startTaskProcessor();
        }

//        /**
//         * Starts a background task processor that:
//         * 1. Picks tasks from the BlockingQueue in submission order
//         * 2. Acquires semaphore for task group
//         * 3. Submits to thread pool for execution
//         */
//        private void startTaskProcessor() {
//            executorService.submit(() -> {
//                while (true) {
//                    try {
//                        // Take task from queue (blocks if empty)
//                        Task<?> task = taskQueue.take();
//
//                        // Get or create semaphore for task group
//                        Semaphore lock = groupLocks.computeIfAbsent(task.taskGroup(), k -> new Semaphore(1));
//
//                        // Submit to executor service with semaphore handling
//                        executorService.submit(() -> {
//                            try {
//                                // Acquire semaphore
//                                lock.acquire();
//
//                                // Execute task
//                                task.taskAction().call();
//                            } catch (InterruptedException e) {
//                                Thread.currentThread().interrupt();
//                                throw new RuntimeException("Interrupted while waiting for semaphore", e);
//                            } catch (Exception e) {
//                                throw new RuntimeException("Task execution failed", e);
//                            } finally {
//                                // Always release semaphore
//                                lock.release();
//                            }
//                        });
//                    } catch (InterruptedException e) {
//                        Thread.currentThread().interrupt();
//                        break;
//                    }
//                }
//            });
//        }

        /**
         * Submits a new task for execution.
         *
         * @param task The task to be executed
         * @return A Future object that can be used to retrieve the task result
         */
        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            // Add task to queue (maintains FIFO order)
            taskQueue.add(task);

            // Submit task for execution - returns immediately with Future
            return executorService.submit(() -> {
                // Get or create semaphore for task group (ensures no concurrent execution within group)
                Semaphore lock = groupLocks.computeIfAbsent(task.taskGroup(), k -> new Semaphore(1));

                // Acquire semaphore - blocks if another task from same group is running
                try {
                    lock.acquire();
                } catch (InterruptedException e) {
                    System.out.println("Interrupted while waiting for semaphore");
                    throw new RuntimeException(e);
                }

                // Execute task - results will be available through the returned Future
                try {
                    try {
                        return task.taskAction().call();
                    } catch (Exception e) {
                        // Wrap any task execution exceptions in runtime exception
                        System.out.println("Error executing task: " + task.taskUUID());
                        throw new RuntimeException(e);
                    }
                } finally {
                    // Always release semaphore when done to allow next task in group to run
                    lock.release();
                }
            });
        }
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID Unique task identifier.
     * @param taskGroup Task group.
     * @param taskType Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T> Task computation result value type.
     */
    public record Task<T>(
    UUID taskUUID,
    TaskGroup taskGroup,
    TaskType taskType,
    Callable<T> taskAction
  ) {
    public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(
            UUID groupUUID
    ) {
    public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

}
