all: 	merge
merge: 	merge.c
	gcc -Wall -g -o merge merge.c -lm -lrt -lpthread 
clean: 
	rm -fr *~ merge *.txt
