package com.sarbesh.opentext;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.sarbesh.opentext.Main.Task;
import com.sarbesh.opentext.Main.TaskGroup;
import com.sarbesh.opentext.Main.TaskType;
import com.sarbesh.opentext.Main.TaskExecutor;
import com.sarbesh.opentext.Main.TaskExecutorImpl;

public class TaskExecutorTest {
    private static final int TIMEOUT = 5000; // 5 seconds timeout for async operations

    public static void main(String[] args) {
        TaskExecutor executor = new TaskExecutorImpl(1);
        try {
            System.out.println("Testing TaskExecutor Implementation...");

            // 1. Test Task Submission (Non-blocking)
            System.out.println("\n1. Testing task submission (non-blocking)");

            long startTime = System.currentTimeMillis();
            Future<?> future1 = executor.submitTask(new Task<>(
                    UUID.randomUUID(),
                    new TaskGroup(UUID.randomUUID()),
                    TaskType.READ,
                    () -> {
                        Thread.sleep(1000);
                        return "result";
                    }
            ));

            Future<?> future2 = executor.submitTask(new Task<>(
                    UUID.randomUUID(),
                    new TaskGroup(UUID.randomUUID()),
                    TaskType.READ,
                    () -> "result2"
            ));

            if (System.currentTimeMillis() - startTime < 100) {
                System.out.println("✅ Task submission is non-blocking");
            } else {
                System.out.println("❌ Task submission is blocking");
            }

            System.out.println("Task 1 result: " + future1.get(TIMEOUT, TimeUnit.MILLISECONDS));
            System.out.println("Task 2 result: " + future2.get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // 2. Test Task Order Preservation
            System.out.println("\n2. Testing task order preservation");
            TaskGroup group = new TaskGroup(UUID.randomUUID());

            Future<String> future3 = executor.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group,
                    TaskType.READ,
                    () -> "first"
            ));

            Future<String> future4 = executor.submitTask(new Main.Task<>(
                    UUID.randomUUID(),
                    group,
                    TaskType.READ,
                    () -> "second"
            ));

            System.out.println("Task 3 result: " + future3.get(TIMEOUT, TimeUnit.MILLISECONDS));
            System.out.println("Task 4 result: " + future4.get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // 3. Test Task Group Concurrency
            System.out.println("\n3. Testing task group concurrency");
            TaskExecutor executor2 = new TaskExecutorImpl(2);
            TaskGroup group = new TaskGroup(UUID.randomUUID());

            Future<String> future5 = executor2.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group,
                    TaskType.READ,
                    () -> {
                        Thread.sleep(1000);
                        return "first";
                    }
            ));

            Future<String> future6 = executor2.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group,
                    TaskType.READ,
                    () -> "second"
            ));

            System.out.println("Task 5 result: " + future5.get(TIMEOUT, TimeUnit.MILLISECONDS));
            System.out.println("Task 6 result: " + future6.get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // 4. Test Concurrent Execution
            System.out.println("\n4. Testing concurrent execution");
            TaskGroup group1 = new TaskGroup(UUID.randomUUID());
            TaskGroup group2 = new TaskGroup(UUID.randomUUID());

            Future<String> future7 = executor.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group1,
                    TaskType.READ,
                    () -> {
                        Thread.sleep(1000);
                        return "task1";
                    }
            ));

            Future<String> future8 = executor.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group2,
                    TaskType.READ,
                    () -> {
                        Thread.sleep(1000);
                        return "task2";
                    }
            ));

            long startTime = System.currentTimeMillis();
            System.out.println("Task 7 result: " + future7.get(TIMEOUT, TimeUnit.MILLISECONDS));
            System.out.println("Task 8 result: " + future8.get(TIMEOUT, TimeUnit.MILLISECONDS));
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Time taken: " + duration + "ms (should be < 3000ms)");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // 5. Test Task Result Availability
            System.out.println("\n5. Testing task result availability");
            Future<String> future9 = executor.submitTask(new Task<>(
                    UUID.randomUUID(),
                    new TaskGroup(UUID.randomUUID()),
                    TaskType.READ,
                    () -> "quick result"
            ));

            System.out.println("Quick task result: " + future9.get(TIMEOUT, TimeUnit.MILLISECONDS));
            System.out.println("✅ Task result availability test passed");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // 6. Test Max Concurrency
            System.out.println("\n6. Testing max concurrency");
            int maxConcurrency = 2;
            TaskExecutor executor2 = new TaskExecutorImpl(maxConcurrency);
            TaskGroup group5 = new TaskGroup(UUID.randomUUID());
            TaskGroup group6 = new TaskGroup(UUID.randomUUID());

            Future<String> future10 = executor2.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group5,
                    TaskType.READ,
                    () -> {
                        Thread.sleep(1000);
                        return "task1";
                    }
            ));

            Future<String> future11 = executor2.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group6,
                    TaskType.READ,
                    () -> {
                        Thread.sleep(1000);
                        return "task2";
                    }
            ));

            Future<String> future12 = executor2.submitTask(new Task<>(
                    UUID.randomUUID(),
                    group5,
                    TaskType.READ,
                    () -> {
                        Thread.sleep(1000);
                        return "task3";
                    }
            ));

            long startTime = System.currentTimeMillis();
            String result10 = future10.get(TIMEOUT, TimeUnit.MILLISECONDS);
            String result11 = future11.get(TIMEOUT, TimeUnit.MILLISECONDS);
            String result12 = future12.get(TIMEOUT, TimeUnit.MILLISECONDS);
            
            if (result10.equals("task1") && result11.equals("task2") && result12.equals("task3") && System.currentTimeMillis() - startTime > 1000) {
                System.out.println("✅ Max concurrency test passed");
            } else {
                System.out.println("❌ Max concurrency test failed");
                throw new RuntimeException("Max concurrency limit not respected");
            }
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("Time taken: " + duration + "ms (should be > 1000ms)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
