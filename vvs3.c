#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

/* функция для обращения порядка элементов в массиве */
void reverse_array(int* arr, int size) {
    int start = 0;
    int end = size - 1;

    // меняем элементы местами, двигаясь от краев к центру
    while (start < end) {
        // классический обмен значений
        int temp = arr[start];
        arr[start] = arr[end];
        arr[end] = temp;

        // сдвигаем индексы к центру
        start++;
        end--;
    }
}

int main(int argc, char** argv) {
    /* переменные для работы с MPI */
    int mpi_check_result;  // для проверки ошибок вызовов MPI
    int numprocs;          // общее количество процессов
    int myid;             // ранг текущего процесса
    int namelen;          // длина имени процессора
    double startwtime, endwtime, send1time, send2time;  // переменные для замера времени
    char processor_name[MPI_MAX_PROCESSOR_NAME];  // имя процессора

    /* инициализация MPI */
    mpi_check_result = MPI_Init(&argc, &argv);
    if (mpi_check_result != MPI_SUCCESS) {
        fprintf(stderr, "ошибка инициализации MPI\n");
        return 1;
    }

    /* получаем общее количество процессов */
    mpi_check_result = MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    if (mpi_check_result != MPI_SUCCESS) {
        fprintf(stderr, "ошибка MPI_Comm_size\n");
        MPI_Finalize();
        return 1;
    }

    /* получаем ранг текущего процесса */
    mpi_check_result = MPI_Comm_rank(MPI_COMM_WORLD, &myid);
    if (mpi_check_result != MPI_SUCCESS) {
        fprintf(stderr, "ошибка MPI_Comm_rank\n");
        MPI_Finalize();
        return 1;
    }

    /* получаем имя процессора для отладки */
    mpi_check_result = MPI_Get_processor_name(processor_name, &namelen);
    if (mpi_check_result != MPI_SUCCESS) {
        fprintf(stderr, "ошибка MPI_Get_processor_name\n");
        MPI_Finalize();
        return 1;
    }

    /* размеры матрицы */
    int num_rows, num_cols;

    /* проверка аргументов командной строки */
    if (argc != 4) {
        if (myid == 0) {  // только root-процесс выводит подсказку
            printf("использование: %s <путь_к_файлу> <строки> <столбцы>\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    /* указатели для работы с файлами и памятью */
    FILE* file = NULL;
    int* numbers = NULL;       // полная матрица (только для root-процесса)
    int* local_numbers = NULL; // часть матрицы для каждого процесса
    int* displs = NULL;       // массив смещений для scatter/gather
    int* rcounts = NULL;      // массив количеств элементов

    /* выделяем память для массивов распределения данных */
    displs = (int*)malloc(numprocs * sizeof(int));
    rcounts = (int*)malloc(numprocs * sizeof(int));

    /* root-процесс (ранг 0) читает входной файл */
    if (myid == 0) {
        file = fopen(argv[1], "r");
        if (!file) {
            fprintf(stderr, "ошибка открытия файла\n");
            MPI_Finalize();
            free(displs);
            free(rcounts);
            return 1;
        }

        /* читаем размеры матрицы из аргументов */
        num_rows = atoi(argv[2]);
        num_cols = atoi(argv[3]);

        /* выделяем память под полную матрицу */
        numbers = (int*)malloc(num_rows * num_cols * sizeof(int));
        if (!numbers) {
            fprintf(stderr, "ошибка выделения памяти\n");
            fclose(file);
            MPI_Finalize();
            free(displs);
            free(rcounts);
            return 1;
        }

        /* читаем элементы матрицы из файла */
        for (int i = 0; i < num_rows * num_cols; ++i) {
            if (fscanf(file, "%d", &numbers[i]) != 1) {
                fprintf(stderr, "ошибка чтения элемента матрицы на позиции %d\n", i);
                fclose(file);
                free(numbers);
                free(displs);
                free(rcounts);
                MPI_Finalize();
                return 1;
            }
        }
        fclose(file);

        /* вычисляем параметры распределения данных */
        int avg_rows = num_rows / numprocs;  // базовое число строк на процесс
        int remainder = num_rows % numprocs; // остаток для распределения
        int offset = 0;

        /* вычисляем количество элементов для каждого процесса */
        for (int i = 0; i < numprocs; ++i) {
            rcounts[i] = avg_rows * num_cols;  // базовое количество
            if (i < remainder) {
                rcounts[i] += num_cols;  // дополнительная строка для первых процессов
            }
            offset += rcounts[i];
        }

        /* вычисляем смещения для каждого процесса */
        offset = 0;
        for (int i = 0; i < numprocs; ++i) {
            displs[i] = offset;
            offset += rcounts[i];
        }
        
        /* начинаем замер времени после завершения настройки */
        startwtime = MPI_Wtime();
    }

    /* рассылаем параметры распределения всем процессам */
    MPI_Bcast(displs, numprocs, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(rcounts, numprocs, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&num_rows, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&num_cols, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    /* замер времени после первой фазы коммуникации */
    send1time = MPI_Wtime();

    /* вычисляем локальный объем работы */
    int local_size = rcounts[myid] / num_cols;  // число строк для этого процесса

    /* выделяем память под локальную часть матрицы */
    local_numbers = (int*)malloc(rcounts[myid] * sizeof(int));
    if (!local_numbers) {
        fprintf(stderr, "ошибка выделения памяти\n");
        free(numbers);
        free(displs);
        free(rcounts);
        MPI_Finalize();
        return 1;
    }

    /* распределяем данные между процессами */
    MPI_Scatterv(numbers, rcounts, displs, MPI_INT, 
                local_numbers, rcounts[myid], MPI_INT, 
                0, MPI_COMM_WORLD);

    /* каждый процесс обрабатывает свои строки */
    for (int i = 0; i < local_size; ++i) {
        int* row = local_numbers + i * num_cols;
        reverse_array(row, num_cols);
    }

    /* отладочный вывод (каждый процесс сообщает о работе) */
    printf("процесс %d/%d на %s работает\n", myid, numprocs, processor_name);

    /* замер времени после вычислений, перед сбором результатов */
    send2time = MPI_Wtime();

    /* собираем результаты в root-процессе */
    MPI_Gatherv(local_numbers, rcounts[myid], MPI_INT, 
                numbers, rcounts, displs, MPI_INT, 
                0, MPI_COMM_WORLD);

    /* синхронизируем процессы перед финальным замером времени */
    MPI_Barrier(MPI_COMM_WORLD);
    endwtime = MPI_Wtime();

    /* root-процесс выводит результаты и время выполнения */
    if (myid == 0) {
        printf("общее время выполнения = %f\n", endwtime - startwtime);
        printf("время передачи 1 = %f\nвремя передачи 2 = %f\nсуммарное время передачи = %f\n", 
               send1time - startwtime, endwtime - send2time, 
               send1time - startwtime + endwtime - send2time);
    }

    /* завершаем работу и освобождаем память */
    MPI_Finalize();
    free(numbers);
    free(local_numbers);
    free(displs);
    free(rcounts);

    return 0;
}