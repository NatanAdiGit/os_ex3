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


}JobContext;

typedef struct ClientHolder {
    const MapReduceClient* client;
    const InputPair* pair;
    const IntermediateVec* libInterVec;
}ClientHolder;


void * ClientMap(void* args) {
    ClientHolder holder = *(ClientHolder*)args;
    holder.client->map(holder.pair->first, holder.pair->second, &holder.libInterVec);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    JobContext jobContext{};

    jobContext.interVec = new IntermediateVec[inputVec.size()];
    jobContext.threads = new pthread_t[multiThreadLevel];
    jobContext.numOfThreads = multiThreadLevel;

    int counter = inputVec.size();
    while (counter > 0)
    {
        for (int i = 0; i < jobContext.numOfThreads && i < counter; ++i)
        {
            ClientHolder clientHolder;
            clientHolder.client = &client;
            clientHolder.pair = &inputVec[i];
            clientHolder.libInterVec = &jobContext.interVec[i];
            pthread_create(jobContext.threads + i, NULL, ClientMap, (void *) &clientHolder);
            counter--;
        }

        for (int i = 0; i < jobContext.numOfThreads; ++i) {
            if (pthread_join(jobContext.threads[i], NULL) != 0) {
                exit(EXIT_FAILURE);
            }
        }



    }





    jobContext.outputVec = &outputVec;
}

void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState* state) {

}

void closeJobHandle(JobHandle job) {

}

void emit2 (K2* key, V2* value, void* context) {

}

void emit3 (K3* key, V3* value, void* context) {

}
