//
// Created by natan on 6/1/2021.
//

#include <pthread.h>
#include <stdlib.h>
#include <atomic>
#include <algorithm>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"

#define HUNDRED_PERCENT 100.0

typedef struct JobContext {
    std::vector<IntermediateVec> *sortedVec;
    std::vector<IntermediateVec> *shuffledVec;
    const MapReduceClient *client;

    IntermediateVec *interVec;
    OutputVec *outputVec;
    int numOfThreads;
    int reduceCounter = 0; //todo probably not good

    pthread_t *threads;

    JobState state;

    pthread_mutex_t emit3;
    std::atomic<int> *atomic_counter;
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
    auto *holder = (MapThreadContext *) args;
    holder->client->map(holder->pair->first, holder->pair->second, &holder->intermediateVec);
    std::sort(holder->intermediateVec->begin(), holder->intermediateVec->end());
    holder->atomic_counter++;
}

void *reduceThread(void *args) {
    auto *holder = (JobContext *) args;
    holder->client->reduce(&holder->interVec[holder->reduceCounter], &holder);
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    auto *jobContext = new JobContext();

    jobContext->sortedVec = new std::vector<IntermediateVec>(inputVec.size());
    //todo release
    jobContext->interVec = new IntermediateVec[inputVec.size()];
    jobContext->threads = new pthread_t[multiThreadLevel];
    jobContext->numOfThreads = multiThreadLevel;
    jobContext->atomic_counter->store(0);
    int counter = inputVec.size();

    while (counter > jobContext->atomic_counter->load()) {
        for (int i = 0; i < jobContext->numOfThreads && i < counter; ++i) {
//            jobContext->sortedVec->push_back(new IntermediateVec());
            MapThreadContext clientHolder;
            clientHolder.client = &client;
            clientHolder.pair = &inputVec[jobContext->atomic_counter->load()];
            clientHolder.intermediateVec = &jobContext->sortedVec->at(jobContext->atomic_counter->load());
            clientHolder.atomic_counter = jobContext->atomic_counter;
            pthread_create(jobContext->threads + i, NULL, mapThread, (void *) &clientHolder);
//            counter--;
        }


        for (int i = 0; i < jobContext->numOfThreads; ++i) {
            if (pthread_join(jobContext->threads[i], NULL) != 0) { // todo - checks its kill the thread
                // todo - print error mess
                exit(EXIT_FAILURE);
            }
//            else {
//                jobContext->state.percentage = jobContext->atomic_counter->load() / (float) inputVec.size();
//            }
        }
    }


    if (jobContext->state.percentage == HUNDRED_PERCENT)
        jobContext->state.stage = SHUFFLE_STAGE;
    else { // it is an error cause we did not finished all the map and sort tasks.
        // todo - print error mess
        exit(EXIT_FAILURE);
    }

    // initialize the atomic counter for the shuffle phase
    jobContext->atomic_counter->store(0);



    jobContext->outputVec = &outputVec;
    return static_cast<JobHandle>(jobContext);
}

void shuffle(void *args) {
    auto *holder = (JobContext *) args;
    if (!holder->sortedVec->empty()) {
        holder->shuffledVec = new std::vector<IntermediateVec>(holder->sortedVec->at(0).size());
        for (long j = holder->sortedVec->at(0).size() - 1; j >= 0; --j)
        {
            for (int i = 0; i < holder->sortedVec->size(); ++i)
            {
                holder->shuffledVec->at(j).push_back(holder->sortedVec->at(i).back());
                holder->sortedVec->at(i).pop_back();
            }
            holder->atomic_counter++;
        }
    }
}


void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState *state) {
    JobContext jobContext = *(JobContext *) job;
    state->stage = jobContext.state.stage;
    state->percentage = jobContext.state.percentage;
}

void closeJobHandle(JobHandle job) {
    //todo destroy mutex
}

void emit2(K2 *key, V2 *value, void *context) {
    auto *vec = (IntermediateVec *) context;
    vec->push_back(IntermediatePair(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    auto *job = (JobContext *) context;
    pthread_mutex_lock(&job->emit3);
    job->outputVec->push_back(OutputPair(key, value));
    pthread_mutex_unlock(&job->emit3);
}


/*
 * questions!
 *
 * to use join in main func? (start job)
 * if so then whats the deal with wait
 *
 * where to update percentage
 */