//
// Created by natan on 6/1/2021.
//

#include <pthread.h>
#include <stdlib.h>
#include "MapReduceFramework.h"


typedef struct JobContext {
    IntermediateVec *interVec;
    OutputVec *outputVec;
    int numOfThreads;
    pthread_t *threads;
    JobState state;
} JobContext;

typedef struct MapThreadContext {
    const MapReduceClient *client;
    const InputPair *pair;
    const IntermediateVec *intermediateVec;
} MapThreadContext;

typedef struct ReduceThreadContext {
    const MapReduceClient *client;
    const IntermediateVec *intermediateVec;
    const OutputVec *outputVec;
} ReduceThreadContext;

void *mapThread(void *args) {
    MapThreadContext holder = *(MapThreadContext *) args;
    holder.client->map(holder.pair->first, holder.pair->second, &holder.intermediateVec);
}

void *reduceThread(void *args) {
    ReduceThreadContext holder = *(ReduceThreadContext *) args;
    holder.client->reduce(holder.intermediateVec, &holder.outputVec);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    JobContext *jobContext = new JobContext();

    jobContext->interVec = new IntermediateVec[inputVec.size()];
    jobContext->threads = new pthread_t[multiThreadLevel];
    jobContext->numOfThreads = multiThreadLevel;

    int counter = inputVec.size();
    while (counter > 0) {
        for (int i = 0; i < jobContext->numOfThreads && i < counter; ++i) {
            MapThreadContext clientHolder;
            clientHolder.client = &client;
            clientHolder.pair = &inputVec[i];
            clientHolder.intermediateVec = &jobContext->interVec[i];
            pthread_create(jobContext->threads + i, NULL, mapThread, (void *) &clientHolder);
            counter--;
        }

        for (int i = 0; i < jobContext->numOfThreads; ++i) {
            if (pthread_join(jobContext->threads[i], NULL) != 0) {
                exit(EXIT_FAILURE);
            }
        }


    }


    jobContext->outputVec = &outputVec;
    return static_cast<JobHandle>(jobContext);
}


void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState *state) {

}

void closeJobHandle(JobHandle job) {

}

void emit2(K2 *key, V2 *value, void *context) {

}

void emit3(K3 *key, V3 *value, void *context) {

}
