#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#define SHMEM_SIZE 4096
#define SEM_KEY    2007
#define SHM_KEY    2007
#define LOG_FILE   "chat_history.txt"
#define SRC_FILE   "QQQ.Q"

union semnum {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
} sem_arg;

struct chat_memory {
    int msg_id;
    pid_t sender_pid;
    char sender_name[64];
    char text[3900];
};

int shm_id = -1, sem_id = -1;
struct chat_memory *shm_buf = (struct chat_memory *)(-1);
pid_t my_pid;
char my_name[64];
pthread_t recv_thread;
int thread_started = 0;

void sem_lock(int semid) {
    struct sembuf sb[1];
    sb[0].sem_num = 0;
    sb[0].sem_op  = -1;
    sb[0].sem_flg = SEM_UNDO;
    if (semop(semid, sb, 1) < 0) {
        exit(1);
    }
}

void sem_unlock(int semid) {
    struct sembuf sb[1];
    sb[0].sem_num = 0;
    sb[0].sem_op  = 1;
    sb[0].sem_flg = SEM_UNDO;
    if (semop(semid, sb, 1) < 0) {
        exit(1);
    }
}

void cleanup(int sig) {
    void *thread_result;

    if (thread_started) {
        pthread_cancel(recv_thread);
        if (!pthread_equal(pthread_self(), recv_thread)) {
            pthread_join(recv_thread, &thread_result);
        }
        if (sig == 0 && thread_result == PTHREAD_CANCELED) {
            fprintf(stderr, "[Поток приема сообщений корректно остановлен (Canceled)]\n");
        }
    }

    if (shm_buf != (struct chat_memory *)(-1)) {
        shmdt(shm_buf);
        shm_buf = (struct chat_memory *)(-1);
    }

    if (shm_id != -1) {
        struct shmid_ds ds;
        if (shmctl(shm_id, IPC_STAT, &ds) == 0 && ds.shm_nattch == 0) {
            shmctl(shm_id, IPC_RMID, NULL);
            if (sem_id != -1) {
                semctl(sem_id, 0, IPC_RMID, sem_arg);
            }
            if (sig == 0) {
                printf("[Разделяемая память и семафоры удалены из системы]\n");
            }
        }
    }

    exit(0);
}

void append_to_log(const char *name, pid_t pid, const char *msg) {
    int fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fd < 0) {
        printf("Can't open log file\n");
        return;
    }
    char line[4096];
    int len = snprintf(line, sizeof(line), "[%s (PID %d)]: %s\n", name, (int)pid, msg);
    (void)write(fd, line, len);
    if (close(fd) < 0) {
        printf("Can't close file\n");
    }
}

void *receiver_thread_func(void *arg) {
    int last_seen_id = *(int *)arg;

    while (1) {
        usleep(100000);

        sem_lock(sem_id);
        if (shm_buf->msg_id > last_seen_id) {
            last_seen_id = shm_buf->msg_id;
            if (shm_buf->sender_pid != my_pid) {
                printf("\r\033[K[%s (PID %d)]: %s\nВы > ",
                       shm_buf->sender_name, (int)shm_buf->sender_pid, shm_buf->text);
                fflush(stdout);
            }
        }
        sem_unlock(sem_id);
    }
    return NULL;
}

void handle_sigint(int sig) {
    fclose(stdin);
}

int main(int argc, char *argv[], char *envp[]) {
    signal(SIGINT, handle_sigint);
    struct shmid_ds ds;
    unsigned short sem_vals[1];
    int is_creator = 0;

    if (argc < 2) {
        fprintf(stderr, "Использование: %s <Ваше_Имя> [--env]\n", argv[0]);
        return 1;
    }
    strncpy(my_name, argv[1], sizeof(my_name) - 1);

    if (argc >= 3 && strcmp(argv[2], "--env") == 0) {
        for (int i = 0; envp[i] != NULL; i++) printf("envp[%d]: %s\n", i, envp[i]);
        for (int i = 0; i < argc; i++) printf("argv[%d]: %s\n", i, argv[i]);
    }

    signal(SIGINT, cleanup);
    signal(SIGHUP, cleanup);
    signal(SIGTERM, cleanup);

    my_pid = getpid();
    pid_t my_ppid = getppid();
    printf("=== Добро пожаловать в чат, %s! (My PID = %d, My PPID = %d) ===\n",
           my_name, (int)my_pid, (int)my_ppid);

    shm_id = shmget(SHM_KEY, SHMEM_SIZE, IPC_CREAT | IPC_EXCL | 0666);
    if (shm_id >= 0) {
        is_creator = 1;
    } else {
        shm_id = shmget(SHM_KEY, SHMEM_SIZE, 0666);
        if (shm_id < 0) {
            fprintf(stderr, "shmget() error\n");
            return 1;
        }
    }

    sem_id = semget(SEM_KEY, 1, 0666 | IPC_CREAT);
    if (sem_id == -1) {
        fprintf(stderr, "semget() error\n");
        return 1;
    }

    shm_buf = (struct chat_memory *)shmat(shm_id, NULL, 0);
    if (shm_buf == (struct chat_memory *)(-1)) {
        fprintf(stderr, "shmat() error\n");
        return 1;
    }

    shmctl(shm_id, IPC_STAT, &ds);
    if (ds.shm_segsz < sizeof(struct chat_memory)) {
        fprintf(stderr, "error: segsize=%zu\n", ds.shm_segsz);
        return 1;
    }

    int active_users = (int)ds.shm_nattch;

    if (is_creator || active_users == 1) {
        sem_vals[0] = 1;
        sem_arg.array = sem_vals;
        if (semctl(sem_id, 0, SETALL, sem_arg) == -1) {
            fprintf(stderr, "semctl() SETALL error\n");
            return 1;
        }
    }

    sem_lock(sem_id);
    if (is_creator || active_users == 1) {
        shm_buf->msg_id = 0;
        shm_buf->sender_pid = 0;
        shm_buf->text[0] = '\0';
    }
    int initial_msg_id = shm_buf->msg_id;
    sem_unlock(sem_id);

    printf("ID памяти: %d | ID семафора: %d | Активных участников: %d\n",
           shm_id, sem_id, active_users);
    printf("Команды: /history (показать лог через fork+exec), /file (отправить файл %s), /exit (выход)\n\n", SRC_FILE);

    if (pthread_create(&recv_thread, NULL, &receiver_thread_func, (void *)&initial_msg_id) != 0) {
        fprintf(stderr, "Error creating thread\n");
        return 1;
    }
    thread_started = 1;

    char input[3900];
    while (1) {
        printf("Вы > ");
        fflush(stdout);

        if (fgets(input, sizeof(input), stdin) == NULL) break;
        input[strcspn(input, "\n")] = '\0';

        if (strlen(input) == 0) continue;

        if (strcmp(input, "/exit") == 0) {
            break;
        }

        if (strcmp(input, "/history") == 0) {
            pid_t chpid = fork();
            if (chpid < 0) {
                printf("Can't fork a child process\n");
            } else if (chpid == 0) {
                printf("\n--- История сообщений (PID потомка: %d, PPID: %d) ---\n",
                       (int)getpid(), (int)getppid());
                int res = execle("/bin/cat", "/bin/cat", LOG_FILE, NULL, envp);
                if (res < 0) {
                    printf("История пока пуста или ошибка запуска /bin/cat\n");
                    exit(-1);
                }
            } else {
                waitpid(chpid, NULL, 0);
                printf("--- Конец истории ---\n");
            }
            continue;
        }

        if (strcmp(input, "/file") == 0) {
            int fd = open(SRC_FILE, O_RDONLY);
            if (fd < 0) {
                printf("Не удалось открыть исходный файл %s (создайте его в текущей папке)\n", SRC_FILE);
                continue;
            }
            sem_lock(sem_id);
            char *p = shm_buf->text;
            int nread, total = 0;
            while ((nread = read(fd, p, 256)) > 0 && (total + nread < (int)sizeof(shm_buf->text) - 1)) {
                p += nread;
                total += nread;
            }
            close(fd);
            *p = '\0';

            shm_buf->sender_pid = my_pid;
            strncpy(shm_buf->sender_name, my_name, sizeof(shm_buf->sender_name) - 1);
            shm_buf->msg_id++;
            append_to_log(my_name, my_pid, shm_buf->text);
            sem_unlock(sem_id);

            printf("[Файл %s отправлен в разделяемую память]\n", SRC_FILE);
            continue;
        }

        sem_lock(sem_id);
        shm_buf->sender_pid = my_pid;
        strncpy(shm_buf->sender_name, my_name, sizeof(shm_buf->sender_name) - 1);
        strcpy(shm_buf->text, input);
        shm_buf->msg_id++;
        append_to_log(my_name, my_pid, input);
        sem_unlock(sem_id);
    }

    cleanup(0);
    return 0;
}

