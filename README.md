# Multithreaded Console Chat in C (Linux IPC)

A real-time console chat application for exchanging messages and files between multiple terminals on Linux (Ubuntu). Built using System V IPC (Shared Memory and Semaphores), POSIX Threads (pthread), process management, and low-level file I/O system calls.

## Implemented Mechanisms

* **Shared Memory (sys/shm.h)** — Creating and attaching a shared memory segment (shmget, shmat), tracking active connected processes via the OS kernel (shmctl, IPC_STAT, shm_nattch), and automatic resource cleanup when the last user exits (shmdt, IPC_RMID).
* **Semaphores (sys/sem.h)** — Process synchronization for safe concurrent access to shared memory (semget, semctl, semop with the SEM_UNDO flag).
* **Multithreading (pthread.h)** — Dedicated background thread (pthread_create) for asynchronous message receiving without blocking console input, with graceful thread cancellation on exit (pthread_cancel, pthread_join).
* **Process Management (unistd.h)** — Displaying PID and PPID (getpid, getppid), spawning a child process (fork), and replacing the process image (execle) to print chat history via /bin/cat.
* **Low-Level File I/O (fcntl.h)** — Reading and broadcasting the contents of QQQ.Q and saving chat logs using open(), read(), write(), and close() system calls.

## Build and Run

1. Compile the source code with the pthread library:

    gcc chat.c -o chat -lpthread

2. Open two (or more) terminal windows in the project directory and run the program, passing your username as a command-line argument:

    ./chat Dima

3. In the second terminal, run another instance:

    ./chat Alex

4. (Optional) To print environment variables (envp) and command-line arguments (argv) on startup, pass the --env flag:

    ./chat Dima --env

## Chat Commands

* <any text> — Immediately broadcast a message to all connected terminals via shared memory.
* /file — Read the contents of QQQ.Q in the current directory and send it to the chat.
* /history — Print the full chat log from chat_history.txt using a child process (fork + execle).
* /exit — Stop the background receiver thread, detach shared memory, and exit cleanly.
