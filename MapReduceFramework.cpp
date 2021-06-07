//
// Created by natan on 6/1/2021.
//

#include <pthread.h>
#include <stdlib.h>
#include <atomic>
#include <algorithm>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"

#define HUNDRED_PERCENT 100.0

typedef struct JobContext {
//    const MapReduceClient *client;

    pthread_t *threads;

    const InputVec *inputVec;

    OutputVec *outputVec;

    std::vector<IntermediateVec> *sortedVec = nullptr;

    std::vector<IntermediateVec> *shuffledVec = nullptr;

    std::atomic<int> *atomic_counter_to_process; // counts how many input pairs were processed
    std::atomic<int> *atomic_counter_stage;
    std::atomic<int> *atomic_counter_processed;

    pthread_mutex_t emit3; //should be the same for all
} JobContext;

typedef struct ThreadContext {
    const MapReduceClient *client;
    int threadID;

    const InputVec *inputVec;
    OutputVec *outputVec;
    std::vector<IntermediateVec> *sortedVec = nullptr;
    std::vector<IntermediateVec> *shuffledVec = nullptr;

    Barrier *barrier;

    std::atomic<int> *atomic_counter_to_process; // checks which input pair to process
    std::atomic<int> *atomic_counter_stage;      // sees what stage
    std::atomic<int> *atomic_counter_processed;  // counts how many input pairs were processed

    pthread_mutex_t emit3; //should be the same for all
} ThreadContext;


void shuffle(ThreadContext *threadContext);

void mapStage(const ThreadContext *threadContext);

void reduceStage(ThreadContext *&threadContext);

void moveToNextStage(const ThreadContext *threadContext, int oldStage, int newStage);

void *thread(void *args) {
    auto *threadContext = (ThreadContext *) args;

    moveToNextStage(threadContext, UNDEFINED_STAGE, MAP_STAGE);

    //map
    mapStage(threadContext);

    //barrier
    threadContext->barrier->barrier();

    moveToNextStage(threadContext, MAP_STAGE, SHUFFLE_STAGE);

    //shuffle
    // only thread 0 does that
    if (threadContext->threadID == 0) {
        shuffle(threadContext);
    }

    //barrier
    threadContext->barrier->barrier();

    moveToNextStage(threadContext, SHUFFLE_STAGE, REDUCE_STAGE);

    //reduce
    reduceStage(threadContext);

}

void moveToNextStage(const ThreadContext *threadContext, int oldStage, int newStage) {
    //todo maybe 'if' not needed
    if (threadContext->atomic_counter_stage->load() == oldStage) {
        threadContext->atomic_counter_to_process->store(0);
        threadContext->atomic_counter_processed->store(0);
        threadContext->atomic_counter_stage->store(newStage);
    }
}

void mapStage(ThreadContext *threadContext) {
//    //todo Nati took it down, we can not initialize the vector for each thread
//    threadContext->sortedVec = new std::vector<IntermediateVec>(threadContext->inputVec->size());

    while (threadContext->atomic_counter_to_process->load() < threadContext->inputVec->size()) {

        // --------- map
        // increment counter
        int old_value = (*(threadContext->atomic_counter_to_process))++;
        auto pair = &threadContext->inputVec->at(old_value);
        auto unSortedInterVec = &threadContext->sortedVec->at(old_value);
        threadContext->client->map(pair->first, pair->second, &unSortedInterVec);

        // --------- sort
        // todo - maybe there will be a problem
        std::sort(unSortedInterVec->begin(), unSortedInterVec->end());

        (*(threadContext->atomic_counter_processed))++;
    }
}

void reduceStage(ThreadContext *&threadContext) {
    while (threadContext->atomic_counter_to_process->load() < threadContext->shuffledVec->size()) {

        // --------- reduce
        // increment counter
        int old_value = (*(threadContext->atomic_counter_to_process))++;
        auto pair = &threadContext->shuffledVec->at(old_value);
        threadContext->client->reduce(pair, &threadContext);

        (*(threadContext->atomic_counter_processed))++;
    }
}

void shuffle(ThreadContext *threadContext) {
    if (threadContext->sortedVec->empty()) {
        //todo something
        exit(5);
    }
//    threadContext->shuffledVec = new std::vector<IntermediateVec>();
//    for (int k = 0; k < threadContext->sortedVec->size(); ++k) {
    while (!threadContext->sortedVec->empty()) {
        auto max = threadContext->sortedVec->at(0).back().first;
        for (int i = 0; i < threadContext->sortedVec->size(); ++i) {
            //find max last key
            if (threadContext->sortedVec->at(i).back().first > max)
                max = threadContext->sortedVec->at(i).back().first;
        }

        IntermediateVec intermediateVec;
        for (int i = 0; i < threadContext->sortedVec->size(); ++i) {
            if (threadContext->sortedVec->at(i).empty())
                continue;
            //insert only if equals to max
            auto currKey = threadContext->sortedVec->at(i).back().first;
            if (!(currKey < max) && !(max < currKey)) { // do not change! it should be like that!
                intermediateVec.push_back(threadContext->sortedVec->at(i).back());
                threadContext->sortedVec->at(i).pop_back();
            }
        }
        threadContext->shuffledVec->insert(threadContext->shuffledVec->begin(), intermediateVec);
        (*(threadContext->atomic_counter_processed))++;
    }


//    for (unsigned long j = threadContext->sortedVec->at(0).size() - 1; j >= 0; --j) {
//        IntermediateVec intermediateVec;
//        for (int i = 0; i < threadContext->sortedVec->size(); ++i) {
//            //todo cant assume they are the same key
//
//            threadContext->shuffledVec->at(j).push_back(threadContext->sortedVec->at(i).back());
//            threadContext->sortedVec->at(i).pop_back();
//        }
//        (*(threadContext->atomic_counter_processed))++;

//    threadContext->shuffledVec = new std::vector<IntermediateVec>(threadContext->sortedVec->at(0).size());
//    for (unsigned long j = threadContext->sortedVec->at(0).size() - 1; j >= 0; --j) {
//        for (int i = 0; i < threadContext->sortedVec->size(); ++i) {
//            //todo cant assume they are the same key
//            threadContext->shuffledVec->at(j).push_back(threadContext->sortedVec->at(i).back());
//            threadContext->sortedVec->at(i).pop_back();
//        }
//        (*(threadContext->atomic_counter_processed))++;
//    }
    }



//typedef struct ThreadContext {
//    const MapReduceClient *client;
//    int threadID;
//
//    const InputVec *inputVec;
//    OutputVec *outputVec;
//    std::vector<IntermediateVec> *sortedVec;
//    std::vector<IntermediateVec> *shuffledVec;
//
//    Barrier *barrier;
//
//    std::atomic<int> *atomic_counter_to_process; // checks which input pair to process
//    std::atomic<int> *atomic_counter_stage;      // sees what stage
//    std::atomic<int> *atomic_counter_processed;  // counts how many input pairs were processed
//
//    pthread_mutex_t emit3; //should be the same for all
//} ThreadContext;


JobHandle startMapReduceJob(const MapReduceClient &client,
                                const InputVec &inputVec, OutputVec &outputVec,
                                int multiThreadLevel) {

        //init job context
        auto *jobContext = new JobContext();
        jobContext->threads = new pthread_t[multiThreadLevel];
        jobContext->inputVec = &inputVec;
        jobContext->sortedVec = new std::vector<IntermediateVec>(inputVec.size());
        jobContext->shuffledVec = new std::vector<IntermediateVec>();
        jobContext->atomic_counter_to_process->store(0);
        jobContext->atomic_counter_stage->store(0);
        jobContext->atomic_counter_processed->store(0);
        // todo - Inbal initialize the  pthread_mutex_t emit3 of the job

        for (int i = 0; i < multiThreadLevel; ++i) {
            auto *newThreadContext = new ThreadContext();
            newThreadContext->client = &client;
            newThreadContext->threadID = i;
            newThreadContext->inputVec = jobContext->inputVec;
            newThreadContext->sortedVec = jobContext->sortedVec;
            newThreadContext->shuffledVec = jobContext->shuffledVec;
            // todo - Inbal initialize the barrier

        }






//    jobContext->inputVec


        //init threads
//    for (int i = 0; i < MT_LEVEL; ++i) {
//        contexts[i] = {i, &atomic_counter};
//    }
//
//    for (int i = 0; i < MT_LEVEL; ++i) {
//        pthread_create(threads + i, NULL, count, contexts + i);
//    }

//    jobContext->outputVec = &outputVec;
        return static_cast<JobHandle>(jobContext);
    }

    void waitForJob(JobHandle job) {

        //joins threads
    }

    void getJobState(JobHandle job, JobState *state) {
        //todo mutex maybe?
        JobContext jobContext = *(JobContext *) job;

        state->stage = static_cast<stage_t>(jobContext.atomic_counter_stage->load());
        float percentage = jobContext.atomic_counter_processed->load();
        switch (jobContext.atomic_counter_stage->load()) {
            case MAP_STAGE:
                percentage /= jobContext.inputVec->size();
                break;
            case SHUFFLE_STAGE:
                percentage = jobContext.shuffledVec->size() == 0 ? 0.0f : percentage / jobContext.shuffledVec->size();
                break;
            case REDUCE_STAGE:
                percentage = jobContext.shuffledVec->size() == 0 ? 0.0f : percentage / jobContext.shuffledVec->size();
                break;
            default:
                percentage = 0.0f;
                break;
        }
        state->percentage = 100.0f * percentage;
    }

    void closeJobHandle(JobHandle job) {
        //todo destroy mutex
    }

    void emit2(K2 *key, V2 *value, void *context) {
        auto *vec = (IntermediateVec *) context;
        vec->push_back(IntermediatePair(key, value));
    }

    void emit3(K3 *key, V3 *value, void *context) {
        auto *job = (ThreadContext *) context;
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