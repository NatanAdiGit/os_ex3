//
// Created by natan on 6/1/2021.
//

#include <pthread.h>
#include <stdlib.h>
#include <atomic>
#include <algorithm>
#include "MapReduceFramework.h"

#define HUNDRED_PERCENT 100.0

typedef struct JobContext {
    IntermediateVec *interVec;
    OutputVec *outputVec;
    int numOfThreads;
    pthread_t *threads;
    JobState state;
    std::atomic<int>* atomic_counter;
} JobContext;

typedef struct MapThreadContext {
    const MapReduceClient *client;
    const InputPair *pair;
    const IntermediateVec *intermediateVec;
    std::atomic<int> *atomic_counter;
} MapThreadContext;

typedef struct ReduceThreadContext {
    const MapReduceClient *client;
    const IntermediateVec *intermediateVec;
    const OutputVec *outputVec;
} ReduceThreadContext;

void *mapThread(void *args) {
    MapThreadContext holder = *(MapThreadContext *) args;
    holder.client->map(holder.pair->first, holder.pair->second, &holder.intermediateVec);
    std::sort(holder.intermediateVec->begin(), holder.intermediateVec->end());
    (*holder.atomic_counter)++;
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
    jobContext->atomic_counter->store(0);

    int counter = inputVec.size();
    while (counter > 0) {
        for (int i = 0; i < jobContext->numOfThreads && i < counter; ++i) {
            MapThreadContext clientHolder;
            clientHolder.client = &client;
            clientHolder.pair = &inputVec[i];
            clientHolder.intermediateVec = &jobContext->interVec[i];
            clientHolder.atomic_counter = jobContext->atomic_counter;
            pthread_create(jobContext->threads + i, NULL, mapThread, (void *) &clientHolder);
            counter--;
        }

        for (int i = 0; i < jobContext->numOfThreads; ++i) {
            if (pthread_join(jobContext->threads[i], NULL) != 0) {
                // todo - print error mess
                exit(EXIT_FAILURE);
            }
            else
            {
                jobContext->state.percentage = jobContext->atomic_counter->load() /
                        (float) inputVec.size();
            }
        }
    }

    if (jobContext->state.percentage == HUNDRED_PERCENT)
        jobContext->state.stage = SHUFFLE_STAGE;
    else { // it is an error cause we did not finished all the map and sort tasks.
        // todo - print error mess
        exit(EXIT_FAILURE);
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
