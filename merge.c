#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>


#define TRACE 0
#define BUFSIZE 100
#define MAXFILENAME 128
#define MAXFILES 16
#define ENDOFDATA -1 /* marks the end of data stream from producer */
#define TRUE 1
#define FALSE 0


/*
	Bounded buffer has a queue of items. It is a FIFO queue. There can
	be at most BUFSIZE items. Below we have structures related to the
	queue and buffer.
*/

struct bb_qelem {
	struct bb_qelem *next;
	int data; /* an item - an integer */
};

struct bb_queue {
	struct bb_qelem *head;
	struct bb_qelem *tail;
	int count; /* number of items in the buffer */
};


struct bounded_buffer {
	struct bb_queue 	*q; 				/* bounded buffer queue */
	pthread_mutex_t 	th_mutex_queue; 	/* mutex to protect queue */
	pthread_cond_t 		th_cond_hasspace; 	/* will cause producer to wait */
	pthread_cond_t 		th_cond_hasitem; 	/* will cause consumer to wait */
};

/***************************************** global variables ***********/
struct bounded_buffer **bbuffer; /* bounded buffer pointer */
FILE * in_file_pointers[MAXFILES];
FILE * out_file_pointer;
/**********************************************************************/

void
bb_queue_init(struct bb_queue *q)
{
	q->count = 0;
	q->head = NULL;
	q->tail = NULL;
}

// this function assumes that space for item is already allocated
void
bb_queue_insert(struct bb_queue *q, struct bb_qelem *qe)
{
	if (q->count == 0) {
		q->head = qe;
		q->tail = qe;
	} else {
		q->tail->next = qe;
		q->tail = qe;
	}

	q->count++;
}


// this function does not free the item
struct bb_qelem *
bb_queue_retrieve(struct bb_queue *q)
{

	struct bb_qelem *qe;
	if (q->count == 0)
		return NULL;

	qe = q->head;
	q->head = q->head->next;
	q->count--;

	return (qe);

}

/*
 	Thread structure
*/
struct arg{
	int isFromFile;
	int isToFile;
	int in_index_first;
	int in_index_second;
	int out_index;
};

void *thread_fcn(void *args)
{
	struct arg * t_arg = ( struct arg *) args;

	if( TRACE )
		printf( "t=%d, in1=%d, in2=%d, o=%d, fromF=%d, toF=%d\n",
				t_arg->out_index, t_arg->in_index_first, t_arg->in_index_second, t_arg->out_index,
				t_arg->isFromFile, t_arg->isToFile);

	int value1, value2, value_to_write;
	struct bb_qelem *qe_new, *qe_first, *qe_second;
	int in1 = t_arg->in_index_first;
	int in2 = t_arg->in_index_second;
	int out = t_arg->out_index;
	int EOF_first, EOF_second;

	// read first values
	if( t_arg->isFromFile == 1 )
	{
		while( (EOF_first = fscanf(in_file_pointers[in1], "%d", &value1)) != EOF && value1 < 0);
		while( (EOF_second = fscanf(in_file_pointers[in2], "%d", &value2)) != EOF && value2 < 0);
	}
	else
	{
		// read value from first buffer
		pthread_mutex_lock(&bbuffer[in1]->th_mutex_queue);
		/* critical section begin */
		while (bbuffer[in1]->q->count == 0) {
			pthread_cond_wait(&bbuffer[in1]->th_cond_hasitem,
								&bbuffer[in1]->th_mutex_queue);
		}
		qe_first = bb_queue_retrieve(bbuffer[in1]->q);
		if (qe_first == NULL) {
			printf("can not retrieve; should not happen\n");
			exit(1);
		}
		if (TRACE)
			printf ("consumer 1 retrieved item = %d\n", qe_first->data);

		if (bbuffer[in1]->q->count == (BUFSIZE - 1))
			pthread_cond_signal(&bbuffer[in1]->th_cond_hasspace);
		/* critical section end */
		pthread_mutex_unlock(&bbuffer[in1]->th_mutex_queue);


		// read value from second buffer
		pthread_mutex_lock(&bbuffer[in2]->th_mutex_queue);
		/* critical section begin */
		while (bbuffer[in2]->q->count == 0) {
			pthread_cond_wait(&bbuffer[in2]->th_cond_hasitem,
								&bbuffer[in2]->th_mutex_queue);
		}
		qe_second = bb_queue_retrieve(bbuffer[in2]->q);
		if (qe_second == NULL) {
			printf("can not retrieve; should not happen\n");
			exit(1);
		}
		if (TRACE)
			printf ("consumer 1 retrieved item = %d\n", qe_second->data);

		if (bbuffer[in2]->q->count == (BUFSIZE - 1))
			pthread_cond_signal(&bbuffer[in2]->th_cond_hasspace);
		/* critical section end */
		pthread_mutex_unlock(&bbuffer[in2]->th_mutex_queue);

		value1 = qe_first->data;
		value2 = qe_second->data;
	}

	while(1)
	{
		if( value1 < value2)
		{
			value_to_write = value1;
			if( t_arg->isFromFile == 1 )
			{
				while( (EOF_first = fscanf(in_file_pointers[in1], "%d", &value1)) != EOF && value1 < 0);
			}
			else
			{
				// read value from first buffer
				pthread_mutex_lock(&bbuffer[in1]->th_mutex_queue);
				/* critical section begin */
				while (bbuffer[in1]->q->count == 0) {
					pthread_cond_wait(&bbuffer[in1]->th_cond_hasitem,
										&bbuffer[in1]->th_mutex_queue);
				}
				qe_first = bb_queue_retrieve(bbuffer[in1]->q);
				if (qe_first == NULL) {
					printf("can not retrieve; should not happen\n");
					exit(1);
				}
				if (TRACE)
					printf ("consumer 1 retrieved item = %d\n", qe_first->data);

				if (bbuffer[in1]->q->count == (BUFSIZE - 1))
					pthread_cond_signal(&bbuffer[in1]->th_cond_hasspace);
				/* critical section end */
				pthread_mutex_unlock(&bbuffer[in1]->th_mutex_queue);

				value1 = qe_first->data;
			}
		}
		else
		{
			value_to_write = value2;

			if( t_arg->isFromFile == 1 )
				while( (EOF_second = fscanf(in_file_pointers[in2], "%d", &value2)) != EOF && value2 < 0);
			else
			{
				pthread_mutex_lock(&bbuffer[in2]->th_mutex_queue);
				/* critical section begin */
				while (bbuffer[in2]->q->count == 0) {
					pthread_cond_wait(&bbuffer[in2]->th_cond_hasitem,
										&bbuffer[in2]->th_mutex_queue);
				}
				qe_second = bb_queue_retrieve(bbuffer[in2]->q);
				if (qe_second == NULL) {
					printf("can not retrieve; should not happen\n");
					exit(1);
				}
				if (TRACE)
					printf ("consumer 1 retrieved item = %d\n", qe_second->data);

				if (bbuffer[in2]->q->count == (BUFSIZE - 1))
					pthread_cond_signal(&bbuffer[in2]->th_cond_hasspace);
				/* critical section end */
				pthread_mutex_unlock(&bbuffer[in2]->th_mutex_queue);

				value2 = qe_second->data;
			}
		}

		if( t_arg->isToFile == 1)
		{
			fprintf(out_file_pointer, "%d\n", value_to_write);
			if( TRACE )
				printf( "Value %d is written to file. t=%d\n", value_to_write, t_arg->out_index);
		}
		else
		{
			/* put and end-of-data marker to the queue */
			qe_new = (struct bb_qelem *) malloc (sizeof (struct bb_qelem));
			if (qe_new == NULL) {
				perror ("malloc failed\n");
				exit (1);
			}
			qe_new->next = NULL;
			qe_new->data = value_to_write;

			pthread_mutex_lock(&bbuffer[out]->th_mutex_queue);
			/* critical section begin */
			while (bbuffer[out]->q->count == BUFSIZE)
				pthread_cond_wait(&bbuffer[out]->th_cond_hasspace,
									&bbuffer[out]->th_mutex_queue);

			bb_queue_insert(bbuffer[out]->q, qe_new);
			if (TRACE)
				printf ("producer insert item = %d\n", qe_new->data);

			if (bbuffer[out]->q->count == 1)
				pthread_cond_signal(&bbuffer[out]->th_cond_hasitem);
			/* critical section end */
			pthread_mutex_unlock(&bbuffer[out]->th_mutex_queue);
		}


		if(t_arg->isFromFile == 1
				&& (EOF_first == EOF || EOF_second == EOF) )
		{
			if(TRACE)
				printf( "breaking");
			break;
		}
		else if( value1 == ENDOFDATA || value2 == ENDOFDATA )
		{
			if(TRACE)
				printf( "breaking");
			break;
		}

	}

	// write remaining ones
	while( (t_arg->isFromFile == 1 && EOF_first != EOF)
			|| (t_arg->isFromFile != 1 && value1 != ENDOFDATA))
	{
		if(TRACE)
			printf( "Writing first file\n");

		if( t_arg->isToFile == 1)
		{
			fprintf(out_file_pointer, "%d\n", value1);
			if( TRACE )
				printf( "Value %d is written to file.\n", value1);
		}
		else
		{
			/* put and end-of-data marker to the queue */
			qe_new = (struct bb_qelem *) malloc (sizeof (struct bb_qelem));
			if (qe_new == NULL) {
				perror ("malloc failed\n");
				exit (1);
			}
			qe_new->next = NULL;
			qe_new->data = value1;

			pthread_mutex_lock(&bbuffer[out]->th_mutex_queue);
			/* critical section begin */
			while (bbuffer[out]->q->count == BUFSIZE)
				pthread_cond_wait(&bbuffer[out]->th_cond_hasspace,
									&bbuffer[out]->th_mutex_queue);

			bb_queue_insert(bbuffer[out]->q, qe_new);
			if (TRACE)
				printf ("producer insert item = %d\n", qe_new->data);

			if (bbuffer[out]->q->count == 1)
				pthread_cond_signal(&bbuffer[out]->th_cond_hasitem);
			/* critical section end */
			pthread_mutex_unlock(&bbuffer[out]->th_mutex_queue);
		}

		if( t_arg->isFromFile == 1)
		{
			while( (EOF_first=fscanf(in_file_pointers[in1], "%d", &value1)) != EOF && value1 < 0);
		}
		else
		{
			pthread_mutex_lock(&bbuffer[in1]->th_mutex_queue);
			/* critical section begin */
			while (bbuffer[in1]->q->count == 0) {
				pthread_cond_wait(&bbuffer[in1]->th_cond_hasitem,
									&bbuffer[in1]->th_mutex_queue);
			}
			qe_first = bb_queue_retrieve(bbuffer[in1]->q);
			if (qe_first == NULL) {
				printf("can not retrieve; should not happen\n");
				exit(1);
			}
			if (TRACE)
				printf ("consumer 1 retrieved item = %d\n", qe_first->data);

			if (bbuffer[in1]->q->count == (BUFSIZE - 1))
				pthread_cond_signal(&bbuffer[in1]->th_cond_hasspace);
			/* critical section end */
			pthread_mutex_unlock(&bbuffer[in1]->th_mutex_queue);

			value1 = qe_first->data;
		}

	}

	while( (t_arg->isFromFile == 1 && EOF_second != EOF)
			|| (t_arg->isFromFile != 1 && value2 != ENDOFDATA))
	{
		if(TRACE)
			printf( "Writing second file\n");

		if( t_arg->isToFile == 1)
		{
			fprintf(out_file_pointer, "%d\n", value2);
			if( TRACE )
				printf( "Value %d is written to file.\n", value2);
		}
		else
		{
			/* put and end-of-data marker to the queue */
			qe_new = (struct bb_qelem *) malloc (sizeof (struct bb_qelem));
			if (qe_new == NULL) {
				perror ("malloc failed\n");
				exit (1);
			}
			qe_new->next = NULL;
			qe_new->data = value2;

			pthread_mutex_lock(&bbuffer[out]->th_mutex_queue);
			/* critical section begin */
			while (bbuffer[out]->q->count == BUFSIZE)
				pthread_cond_wait(&bbuffer[out]->th_cond_hasspace,
									&bbuffer[out]->th_mutex_queue);

			bb_queue_insert(bbuffer[out]->q, qe_new);
			if (TRACE)
				printf ("producer insert item = %d\n", qe_new->data);

			if (bbuffer[out]->q->count == 1)
				pthread_cond_signal(&bbuffer[out]->th_cond_hasitem);
			/* critical section end */
			pthread_mutex_unlock(&bbuffer[out]->th_mutex_queue);
		}

		if( t_arg->isFromFile == 1)
		{
			while( (EOF_second=fscanf(in_file_pointers[in2], "%d", &value2)) != EOF && value2 < 0);
		}
		else
		{
			pthread_mutex_lock(&bbuffer[in2]->th_mutex_queue);
			/* critical section begin */
			while (bbuffer[in2]->q->count == 0) {
				pthread_cond_wait(&bbuffer[in2]->th_cond_hasitem,
									&bbuffer[in2]->th_mutex_queue);
			}
			qe_first = bb_queue_retrieve(bbuffer[in2]->q);
			if (qe_first == NULL) {
				printf("can not retrieve; should not happen\n");
				exit(1);
			}
			if (TRACE)
				printf ("consumer 1 retrieved item = %d\n", qe_first->data);

			if (bbuffer[in2]->q->count == (BUFSIZE - 1))
				pthread_cond_signal(&bbuffer[in2]->th_cond_hasspace);
			/* critical section end */
			pthread_mutex_unlock(&bbuffer[in2]->th_mutex_queue);

			value2 = qe_first->data;
		}

	}

	if( t_arg->isToFile != 1)
	{
		/* put and end-of-data marker to the queue */
		qe_new = (struct bb_qelem *) malloc (sizeof (struct bb_qelem));
		if (qe_new == NULL) {
			perror ("malloc failed\n");
			exit (1);
		}
		qe_new->next = NULL;
		qe_new->data = ENDOFDATA;

		pthread_mutex_lock(&bbuffer[out]->th_mutex_queue);
		/* critical section begin */
		while (bbuffer[out]->q->count == BUFSIZE)
			pthread_cond_wait(&bbuffer[out]->th_cond_hasspace,
								&bbuffer[out]->th_mutex_queue);

		bb_queue_insert(bbuffer[out]->q, qe_new);
		if (TRACE)
			printf ("producer %d insert item = %d\n", t_arg->out_index, qe_new->data);

		if (bbuffer[out]->q->count == 1)
			pthread_cond_signal(&bbuffer[out]->th_cond_hasitem);
		/* critical section end */
		pthread_mutex_unlock(&bbuffer[out]->th_mutex_queue);
	}

	if( TRACE )
		printf( "thread %d exited\n", t_arg->out_index);
	pthread_exit(0);
}

int main( int argc, char ** argv)
{
	int i, j;
	struct arg args[MAXFILES-1];
	pthread_t tids[MAXFILES-1];
	int in_file_count;
	int output_thread_level;
	int thread_count;
	int sub_t_count;
	int buffer_count;
	int ret;

	in_file_count = atoi(argv[1]);
	buffer_count = in_file_count - 2;
	output_thread_level = log2(in_file_count) - 1; // thread level indices starts from zero


	// initialize file pointers
	out_file_pointer = fopen(argv[2], "w");
	for( i = 0; i < in_file_count; i++)
		in_file_pointers[i] = fopen(argv[3+i], "r");

	// initialize buffers
	bbuffer = (struct bounded_buffer **)
					  malloc( buffer_count * sizeof (struct bounded_buffer*));
	for( i = 0; i < buffer_count; i++)
	{
		bbuffer[i] = (struct bounded_buffer *)
							malloc(sizeof (struct bounded_buffer));
		bbuffer[i]->q = (struct bb_queue *) malloc(sizeof (struct bb_queue));
		bb_queue_init(bbuffer[i]->q );
		pthread_mutex_init(&bbuffer[i]->th_mutex_queue, NULL);
		pthread_cond_init(&bbuffer[i]->th_cond_hasspace, NULL);
		pthread_cond_init(&bbuffer[i]->th_cond_hasitem, NULL);
	}

	// initialize threads
	thread_count = 0;
	buffer_count = 0;
	for( i = 0; i <= output_thread_level; i++)
	{
		sub_t_count = in_file_count / pow(2, i+1);
		if( TRACE)
			printf("sub_t_count = %d\n", sub_t_count);

		for( j = 0; j < sub_t_count; j++)
		{
			args[thread_count].isFromFile = 0;
			args[thread_count].isToFile = 0;

			if( i == 0 )
			{
				args[thread_count].in_index_first = 2 * j;
				args[thread_count].in_index_second = 2 * j + 1;
				args[thread_count].isFromFile = 1;
			}
			else
			{
				args[thread_count].in_index_first = buffer_count++;
				args[thread_count].in_index_second = buffer_count++;
			}

			if( i == output_thread_level)
				args[thread_count].isToFile = 1;

			args[thread_count].out_index = thread_count;

			ret = pthread_create(&(tids[thread_count]), NULL, thread_fcn, (void *) &(args[thread_count]));
			if (ret != 0) {
				printf("Error: pthread_create failed.\n");
				exit(1);
			}

			thread_count++;
		}
	}

	for( i = 0; i < thread_count; i++)
	{
		ret = pthread_join(tids[i], NULL);
		if (ret != 0) {
			printf("Error: pthread_join failed.\n");
			exit(1);
		}
	}

	if( TRACE )
		printf("all threads joined\n");

	return 0;

}












