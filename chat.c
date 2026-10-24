#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <termios.h>
#include <time.h>

#define SHMEM_SIZE 4096
#define SEM_KEY 2007
#define SHM_KEY 2007
#define MSG_KEY 2008
#define LOG_FILE "chat_history.txt"
#define SRC_FILE "QQQ.Q"
#define CLR_RESET "\033[0m"
#define CLR_GREEN "\033[1;32m"
#define CLR_CYAN "\033[1;36m"
#define CLR_YELLOW "\033[1;33m"
#define CLR_GRAY "\033[0;90m"

char current_input[3900] = {0};
int input_pos = 0;
struct termios orig_termios;

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

struct pm_msg {
    long mtype;
    char sender_name[64];
    char text[3900];
};

int shm_id = -1, sem_id = -1, msg_qid = -1;
struct chat_memory *shm_buf = (struct chat_memory *)(-1);
pid_t my_pid;
char my_name[64];
pthread_t recv_thread;
int thread_started = 0;

// Генерация уникального mtype на основе имени пользователя
long hash_name(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = (unsigned char)*str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return (hash % 1000000) + 1; // mtype всегда должен быть > 0
}

void sem_lock(int semid) {
    struct sembuf sb[1];
    sb[0].sem_num = 0;
    sb[0].sem_op = -1;
    sb[0].sem_flg = SEM_UNDO;
    if (semop(semid, sb, 1) < 0) {
        exit(1);
    }
}

void sem_unlock(int semid) {
    struct sembuf sb[1];
    sb[0].sem_num = 0;
    sb[0].sem_op = 1;
    sb[0].sem_flg = SEM_UNDO;
    if (semop(semid, sb, 1) < 0) {
        exit(1);
    }
}

void get_time_str(char *buf, size_t size) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    strftime(buf, size, "%H:%M:%S", t);
}

void disable_raw_mode() {
    tcsetattr(STDIN_FILENO, TCSANOW, &orig_termios);
}

void enable_raw_mode() {
    tcgetattr(STDIN_FILENO, &orig_termios);
    atexit(disable_raw_mode);
    struct termios raw = orig_termios;
    raw.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &raw);
}

int read_chat_line(char *out_buf, int max_len) {
    input_pos = 0;
    current_input[0] = '\0';
    printf(CLR_GREEN "You > " CLR_RESET);
    fflush(stdout);

    char c;
    while (read(STDIN_FILENO, &c, 1) == 1) {
        if (c == '\n' || c == '\r') {
            current_input[input_pos] = '\0';
            strcpy(out_buf, current_input);
            input_pos = 0;
            current_input[0] = '\0';
            return 1;
        } else if (c == 127 || c == '\b') {
            if (input_pos > 0) {
                input_pos--;
                while (input_pos > 0 && (current_input[input_pos] & 0xC0) == 0x80) {
                    input_pos--;
                }
                current_input[input_pos] = '\0';
                printf("\r\033[K" CLR_GREEN "You > " CLR_RESET "%s", current_input);
                fflush(stdout);
            }
        } else if ((unsigned char)c >= 32 && input_pos < max_len - 1) {
            current_input[input_pos++] = c;
            current_input[input_pos] = '\0';
            write(STDOUT_FILENO, &c, 1);
        }
    }
    return 0;
}

void cleanup(int sig) {
    void *thread_result;

    if (sig == 0 && thread_started) {
        pthread_cancel(recv_thread);
        if (!pthread_equal(pthread_self(), recv_thread)) {
            pthread_join(recv_thread, &thread_result);
        }
        if (thread_result == PTHREAD_CANCELED) {
            fprintf(stderr, "[Receiver thread cleanly stopped (Canceled)]\n");
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
            if (msg_qid != -1) {
                msgctl(msg_qid, IPC_RMID, NULL);
            }
            remove(LOG_FILE);
            if (sig == 0) {
                printf("[IPC Resources removed from system]\n");
            }
        }
    }

    disable_raw_mode();
    exit(0);
}

void append_to_log(const char *name, pid_t pid, const char *msg) {
    int fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fd < 0) {
        return;
    }
    char line[4096];
    int len = snprintf(line, sizeof(line), "[%s (PID %d)]: %s\n", name, (int)pid, msg);
    (void)write(fd, line, len);
    close(fd);
}

void *receiver_thread_func(void *arg) {
    int last_seen_id = *(int *)arg;
    char tbuf[16];
    long my_mtype = hash_name(my_name);
    struct pm_msg pmsg;

    while (1) {
        usleep(100000);

        // Читаем личные сообщения (IPC_NOWAIT позволяет не блокировать поток)
        while (msgrcv(msg_qid, &pmsg, sizeof(pmsg) - sizeof(long), my_mtype, IPC_NOWAIT) > 0) {
            get_time_str(tbuf, sizeof(tbuf));
            printf("\r\033[K" CLR_GRAY "[%s]" CLR_YELLOW " [PM from %s]:" CLR_RESET " %s\n" CLR_GREEN "You > " CLR_RESET "%s",
                   tbuf, pmsg.sender_name, pmsg.text, current_input);
            fflush(stdout);
        }

        // Читаем глобальный чат
        sem_lock(sem_id);
        if (shm_buf->msg_id > last_seen_id) {
            last_seen_id = shm_buf->msg_id;
            if (shm_buf->sender_pid != my_pid) {
                get_time_str(tbuf, sizeof(tbuf));
                printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_CYAN "[%s (PID %d)]:" CLR_RESET " %s\n" CLR_GREEN "You > " CLR_RESET "%s",
                       tbuf, shm_buf->sender_name, (int)shm_buf->sender_pid, shm_buf->text, current_input);
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
        fprintf(stderr, "Usage: %s <Your_Name> [--env]\n", argv[0]);
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
    
    // Инициализация IPC: Shared Memory, Semaphores, Message Queue
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

    msg_qid = msgget(MSG_KEY, IPC_CREAT | 0666);
    if (msg_qid == -1) {
        fprintf(stderr, "msgget() error\n");
        return 1;
    }

    shm_buf = (struct chat_memory *)shmat(shm_id, NULL, 0);
    if (shm_buf == (struct chat_memory *)(-1)) {
        fprintf(stderr, "shmat() error\n");
        return 1;
    }

    shmctl(shm_id, IPC_STAT, &ds);
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
        close(open(LOG_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0666));
    }
    int initial_msg_id = shm_buf->msg_id;
    sem_unlock(sem_id);

    printf(CLR_YELLOW "============================================================\n");
    printf(" CHAT STARTED | User: %-15s | PID: %d (PPID: %d)\n", my_name, (int)my_pid, (int)my_ppid);
    printf(" SHM ID: %-4d | SEM ID: %-4d           | Active Users: %d\n", shm_id, sem_id, active_users);
    printf(" MSG QID: %-3d |\n", msg_qid);
    printf("------------------------------------------------------------\n");
    printf(" Commands: /history, /file, /calc <num>, /pm <Name> <Msg>, /exit\n");
    printf("============================================================\n\n" CLR_RESET);

    if (pthread_create(&recv_thread, NULL, &receiver_thread_func, (void *)&initial_msg_id) != 0) {
        fprintf(stderr, "Error creating thread\n");
        return 1;
    }
    thread_started = 1;

    enable_raw_mode();

    char input[3900];
    char tbuf[16];
    while (read_chat_line(input, sizeof(input))) {
        if (strlen(input) == 0) {
            printf("\r\033[K");
            continue;
        }

        get_time_str(tbuf, sizeof(tbuf));

        if (strcmp(input, "/exit") == 0) {
            break;
        }

        if (strncmp(input, "/pm ", 4) == 0) {
            char target_name[64];
            char pm_text[3900];
            
            if (sscanf(input + 4, "%63s %[^\n]", target_name, pm_text) == 2) {
                struct pm_msg pmsg;
                pmsg.mtype = hash_name(target_name);
                strncpy(pmsg.sender_name, my_name, sizeof(pmsg.sender_name));
                strcpy(pmsg.text, pm_text);
                
                msgsnd(msg_qid, &pmsg, sizeof(pmsg) - sizeof(long), 0);
                
                printf("\r\033[K" CLR_GRAY "[%s]" CLR_YELLOW " [PM to %s]:" CLR_RESET " %s\n", tbuf, target_name, pm_text);
            } else {
                printf("\r\033[K" CLR_YELLOW "[System]: Usage: /pm <Name> <Text>\n" CLR_RESET);
            }
            continue;
        }

        if (strncmp(input, "/calc ", 6) == 0) {
            printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_GREEN "[You]:" CLR_RESET " %s\n", tbuf, input);
            int num = atoi(input + 6);
            int pipe_to_child[2], pipe_to_parent[2];
            
            if (pipe(pipe_to_child) < 0 || pipe(pipe_to_parent) < 0) {
                printf(CLR_YELLOW "Pipe creation error\n" CLR_RESET);
                continue;
            }
            
            pid_t calc_pid = fork();
            if (calc_pid < 0) {
                printf(CLR_YELLOW "Fork error\n" CLR_RESET);
            } else if (calc_pid == 0) { 
                close(pipe_to_child[1]); 
                close(pipe_to_parent[0]); 
                
                int received_num;
                if (read(pipe_to_child[0], &received_num, sizeof(int)) > 0) {
                    int result = received_num * received_num;
                    write(pipe_to_parent[1], &result, sizeof(int));
                }
                
                close(pipe_to_child[0]);
                close(pipe_to_parent[1]);
                _exit(0); 
            } else { 
                close(pipe_to_child[0]);  
                close(pipe_to_parent[1]); 
                
                write(pipe_to_child[1], &num, sizeof(int));
                
                int final_result;
                if (read(pipe_to_parent[0], &final_result, sizeof(int)) > 0) {
                    printf(CLR_YELLOW "[System]: Result of /calc: %d^2 = %d\n" CLR_RESET, num, final_result);
                }
                
                close(pipe_to_child[1]);
                close(pipe_to_parent[0]);
            }
            continue;
        }

        if (strcmp(input, "/history") == 0) {
            printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_GREEN "[You]:" CLR_RESET " %s\n", tbuf, input);
            pid_t chpid = fork();
            if (chpid < 0) {
                printf("Can't fork a child process\n");
            } else if (chpid == 0) {
                printf("\n--- Message History (Child PID: %d, PPID: %d) ---\n",
                       (int)getpid(), (int)getppid());
                int res = execle("/bin/cat", "/bin/cat", LOG_FILE, NULL, envp);
                if (res < 0) {
                    printf("History is empty or /bin/cat failed to start\n");
                    exit(-1);
                }
            } else {
                waitpid(chpid, NULL, 0);
                printf("--- End of History ---\n");
            }
            continue;
        }

        if (strcmp(input, "/file") == 0) {
            printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_GREEN "[You]:" CLR_RESET " %s\n", tbuf, input);
            int fd = open(SRC_FILE, O_RDONLY);
            if (fd < 0) {
                printf("Failed to open source file %s\n", SRC_FILE);
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

            printf("[File %s sent to shared memory]\n", SRC_FILE);
            continue;
        }

        printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_GREEN "[You]:" CLR_RESET " %s\n", tbuf, input);
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
